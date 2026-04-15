import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from hqzcsj.routes.pcsjqajtj_routes import pcsjqajtj_bp


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


class TestPcsjqajtjRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(pcsjqajtj_bp)

        @app.route("/login")
        def login():
            return "login"

        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_summary_requires_login(self) -> None:
        response = self.client.get("/pcsjqajtj/api/summary")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_summary_uses_default_time_range_and_filters(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.pcsjqajtj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.pcsjqajtj_routes.pcsjqajtj_service.default_time_range_for_page",
            return_value=("2026-04-01 00:00:00", "2026-04-08 00:00:00"),
        ), patch(
            "hqzcsj.routes.pcsjqajtj_routes.pcsjqajtj_service.build_summary",
            return_value=(
                SimpleNamespace(start_time="2026-04-01 00:00:00", end_time="2026-04-08 00:00:00"),
                [{"派出所": "城中所", "警情": 4}],
            ),
        ) as mock_build_summary:
            response = self.client.get("/pcsjqajtj/api/summary?leixing=治安&ssfjdm=445302")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["rows"][0]["警情"], 4)
        self.assertEqual(mock_build_summary.call_args.kwargs["start_time"], "2026-04-01 00:00:00")
        self.assertEqual(mock_build_summary.call_args.kwargs["end_time"], "2026-04-08 00:00:00")
        self.assertEqual(mock_build_summary.call_args.kwargs["leixing_list"], ["治安"])
        self.assertEqual(mock_build_summary.call_args.kwargs["ssfjdm_list"], ["445302"])

    def test_detail_page_renders_context_from_filters(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.pcsjqajtj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.pcsjqajtj_routes.pcsjqajtj_service.fetch_detail",
            return_value=([{"姓名": "李四"}], False),
        ), patch(
            "hqzcsj.routes.pcsjqajtj_routes.render_template",
            return_value="ok",
        ) as mock_render:
            response = self.client.get(
                "/pcsjqajtj/detail"
                "?metric=%E8%AD%A6%E6%83%85"
                "&pcsdm=445302001"
                "&pcs_name=%E5%9F%8E%E4%B8%AD%E6%89%80"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
                "&leixing=%E6%B2%BB%E5%AE%89"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_render.call_args.args[0], "pcsjqajtj_detail.html")
        self.assertEqual(mock_render.call_args.kwargs["metric"], "警情")
        self.assertEqual(mock_render.call_args.kwargs["pcsdm"], "445302001")
        self.assertEqual(mock_render.call_args.kwargs["pcs_name"], "城中所")
        self.assertEqual(mock_render.call_args.kwargs["leixing_list"], ["治安"])
        self.assertFalse(mock_render.call_args.kwargs["truncated"])

    def test_export_summary_uses_provided_column_order(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.pcsjqajtj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ):
            response = self.client.post(
                "/pcsjqajtj/export",
                json={
                    "fmt": "csv",
                    "rows": [{"B列": 2, "A列": 1}],
                    "columns": ["B列", "A列"],
                    "start_time": "2026-04-01 00:00:00",
                    "end_time": "2026-04-08 00:00:00",
                    "selected_fenju_names": ["云城分局"],
                },
            )

        body = response.data.decode("utf-8-sig")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.headers["Content-Type"])
        self.assertTrue(body.startswith("B列,A列"))


if __name__ == "__main__":
    unittest.main()