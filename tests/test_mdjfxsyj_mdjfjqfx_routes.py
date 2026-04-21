import unittest
from unittest.mock import patch

from flask import Flask, Response, session

from mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes import mdjfxsyj_mdjfjqfx_bp


class _Cursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, row=(1,)):
        self.row = row
        self.closed = False

    def cursor(self):
        return _Cursor(self.row)

    def close(self):
        self.closed = True


class TestMdjfjqfxRoutes(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"

        @app.get("/login")
        def login():
            return "login"

        app.register_blueprint(mdjfxsyj_mdjfjqfx_bp, url_prefix="/mdjfxsyj/mdjfjqfx")
        self.app = app
        self.client = app.test_client()

    def _login(self):
        with self.client.session_transaction() as sess:
            sess["username"] = "tester"

    def test_requires_login(self):
        response = self.client.get("/mdjfxsyj/mdjfjqfx/api/options")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_forbids_user_without_permission(self):
        self._login()
        with patch(
            "mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.get_database_connection",
            return_value=_Connection(row=None),
        ):
            response = self.client.get("/mdjfxsyj/mdjfjqfx/api/options")

        self.assertEqual(response.status_code, 403)

    def test_api_summary_parses_filters(self):
        self._login()
        fake_payload = {
            "start_time": "2026-04-01 00:00:00",
            "end_time": "2026-04-02 00:00:00",
            "overall": [],
            "fine": [],
            "repeat": [],
        }
        with (
            patch("mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.get_database_connection", return_value=_Connection()),
            patch("mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.get_summary_payload", return_value=fake_payload) as mock_summary,
        ):
            response = self.client.get(
                "/mdjfxsyj/mdjfjqfx/api/summary"
                "?start_time=2026-04-01%2000:00:00&end_time=2026-04-02%2000:00:00"
                "&group_by=paichusuo&repeat_min=3&ssfjdm=B1&ssfjdm=B2"
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(mock_summary.call_args.kwargs["ssfjdm_list"], ["B1", "B2"])
        self.assertEqual(mock_summary.call_args.kwargs["group_by"], "paichusuo")
        self.assertEqual(mock_summary.call_args.kwargs["repeat_min"], 3)

    def test_invalid_repeat_min_returns_400(self):
        self._login()
        with patch("mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.get_database_connection", return_value=_Connection()):
            response = self.client.get("/mdjfxsyj/mdjfjqfx/api/summary?repeat_min=11")

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.get_json()["success"])

    def test_export_summary_uses_xlsx_response(self):
        self._login()
        with (
            patch("mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.get_database_connection", return_value=_Connection()),
            patch(
                "mdjfxsyj.routes.mdjfxsyj_mdjfjqfx_routes.build_summary_export",
                return_value=Response(b"xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            ) as mock_export,
        ):
            response = self.client.get("/mdjfxsyj/mdjfjqfx/export/summary?repeat_min=2")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"xlsx")
        self.assertEqual(mock_export.call_args.kwargs["repeat_min"], 2)


if __name__ == "__main__":
    unittest.main()
