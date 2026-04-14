import unittest
from unittest.mock import patch

from flask import Flask

from hqzcsj.routes.wcnr_10lv_routes import wcnr_10lv_bp


class _DummyCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, _sql, _params=None) -> None:
        return None

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConnection:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _DummyCursor(self._row)

    def close(self) -> None:
        return None


class TestWcnr10lvRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(wcnr_10lv_bp)

        @app.route("/login")
        def login():
            return "login"

        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_summary_requires_login(self) -> None:
        response = self.client.get("/wcnr_10lv/api/summary")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_summary_returns_new_place_columns(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.wcnr_10lv_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.wcnr_10lv_routes.wcnr_10lv_service.build_summary",
            return_value=(
                {
                    "start_time": "2026-04-01 00:00:00",
                    "end_time": "2026-04-08 00:00:00",
                    "yoy_start_time": "2025-04-01 00:00:00",
                    "yoy_end_time": "2025-04-08 00:00:00",
                    "hb_start_time": "2026-03-25 00:00:00",
                    "hb_end_time": "2026-04-01 00:00:00",
                    "flags": {},
                    "hb_loaded": True,
                },
                [
                    {
                        "地区": "云城",
                        "警情": 4,
                        "警情(场所)": 1,
                        "案件(场所)": 2,
                    }
                ],
            ),
        ) as mock_build_summary, patch(
            "hqzcsj.routes.wcnr_10lv_routes.wcnr_10lv_service.get_display_columns",
            return_value=["地区", "警情", "警情(场所)", "案件(场所)"],
        ) as mock_get_display_columns:
            response = self.client.get(
                "/wcnr_10lv/api/summary"
                "?show_hb=1"
                "&show_ratio=1"
                "&profile=1"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
                "&leixing=%E6%B2%BB%E5%AE%89"
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["columns"], ["地区", "警情", "警情(场所)", "案件(场所)"])
        self.assertEqual(payload["rows"][0]["警情(场所)"], 1)
        self.assertEqual(payload["rows"][0]["案件(场所)"], 2)
        self.assertEqual(mock_build_summary.call_args.kwargs["leixing_list"], ["治安"])
        self.assertTrue(mock_build_summary.call_args.kwargs["include_hb"])
        self.assertTrue(mock_build_summary.call_args.kwargs["include_perf"])
        self.assertTrue(mock_get_display_columns.called)

    def test_api_detail_accepts_new_place_metric(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.wcnr_10lv_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.wcnr_10lv_routes.wcnr_10lv_service.fetch_detail",
            return_value=[{"地区": "云安", "案件编号": "A1"}],
        ) as mock_fetch_detail:
            response = self.client.get(
                "/wcnr_10lv/api/detail"
                "?metric=aj_changsuo"
                "&part=value"
                "&period=current"
                "&diqu=445303"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["rows"][0]["案件编号"], "A1")
        self.assertEqual(mock_fetch_detail.call_args.kwargs["metric"], "aj_changsuo")
        self.assertEqual(mock_fetch_detail.call_args.kwargs["diqu"], "445303")


if __name__ == "__main__":
    unittest.main()
