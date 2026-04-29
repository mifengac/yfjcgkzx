import unittest
from unittest.mock import patch

from flask import Flask, Response

from hqzcsj.routes.tqzmjy_tj_routes import tqzmjy_tj_bp


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


class TestTqzmjyTjRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(tqzmjy_tj_bp)

        @app.route("/login")
        def login():
            return "login"

        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_query_requires_login(self) -> None:
        response = self.client.get("/tqzmjy_tj/api/query")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_query_uses_default_time_range_and_filters(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.tqzmjy_tj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.tqzmjy_tj_routes.tqzmjy_tj_service.default_time_range_for_page",
            return_value=("2026-04-01 00:00:00", "2026-04-08 00:00:00"),
        ), patch(
            "hqzcsj.routes.tqzmjy_tj_routes.tqzmjy_tj_service.query_rows",
            return_value=(
                {"start_time": "2026-04-01 00:00:00", "end_time": "2026-04-08 00:00:00"},
                [{"案件编号": "A001"}],
            ),
        ) as mock_query:
            response = self.client.get("/tqzmjy_tj/api/query?leixing=治安&ssfjdm=445302")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["rows"][0]["案件编号"], "A001")
        self.assertEqual(mock_query.call_args.kwargs["start_time"], "2026-04-01 00:00:00")
        self.assertEqual(mock_query.call_args.kwargs["end_time"], "2026-04-08 00:00:00")
        self.assertEqual(mock_query.call_args.kwargs["leixing_list"], ["治安"])
        self.assertEqual(mock_query.call_args.kwargs["ssfjdm_list"], ["445302"])

    def test_export_uses_required_filename_pattern(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.tqzmjy_tj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.tqzmjy_tj_routes.tqzmjy_tj_service.query_rows",
            return_value=(
                {"start_time": "2026-04-01 00:00:00", "end_time": "2026-04-08 00:00:00"},
                [{"案件编号": "A001"}],
            ),
        ), patch(
            "hqzcsj.routes.tqzmjy_tj_routes._download_csv",
            return_value=Response(b"csv"),
        ) as mock_download:
            response = self.client.get(
                "/tqzmjy_tj/export"
                "?fmt=csv"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"csv")
        filename = mock_download.call_args.args[1]
        self.assertTrue(filename.startswith("2026-04-01-00-00-00至2026-04-08-00-00-00提请专门教育申请书"))
        self.assertTrue(filename.endswith(".csv"))


if __name__ == "__main__":
    unittest.main()