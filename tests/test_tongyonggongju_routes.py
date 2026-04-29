from __future__ import annotations

import io
import unittest
from unittest.mock import patch

from flask import Flask

from tongyonggongju.routes.tongyonggongju_routes import tongyonggongju_bp


class _Cursor:
    def __init__(self, row=(1,)):
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


class TestTongyonggongjuRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"

        @app.get("/login")
        def login():
            return "login"

        app.register_blueprint(tongyonggongju_bp, url_prefix="/tongyonggongju")
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as sess:
            sess["username"] = "tester"

    def test_requires_login(self) -> None:
        response = self.client.get("/tongyonggongju/")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_forbids_user_without_permission(self) -> None:
        self._login()
        with patch(
            "tongyonggongju.routes.tongyonggongju_routes.get_database_connection",
            return_value=_Connection(row=None),
        ):
            response = self.client.get("/tongyonggongju/")

        self.assertEqual(response.status_code, 403)

    def test_upload_sets_session_token(self) -> None:
        self._login()
        upload_payload = {
            "token": "abc123456789012345",
            "filename": "名单.xlsx",
            "sheet_name": "Sheet",
            "columns": [{"index": 1, "display": "A - 身份证号"}],
        }
        with (
            patch("tongyonggongju.routes.tongyonggongju_routes.get_database_connection", return_value=_Connection()),
            patch(
                "tongyonggongju.routes.tongyonggongju_routes.inspect_and_store_workbook",
                return_value=upload_payload,
            ) as mock_inspect,
        ):
            response = self.client.post(
                "/tongyonggongju/api/background/upload",
                data={"file": (io.BytesIO(b"xlsx"), "名单.xlsx")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(mock_inspect.call_args.args[1], "名单.xlsx")
        with self.client.session_transaction() as sess:
            self.assertEqual(sess["tygj_background_token"], "abc123456789012345")

    def test_check_uses_session_token(self) -> None:
        self._login()
        with self.client.session_transaction() as sess:
            sess["tygj_background_token"] = "abc123456789012345"

        fake_result = {"stats": {}, "overview": [], "details": {}}
        with (
            patch("tongyonggongju.routes.tongyonggongju_routes.get_database_connection", return_value=_Connection()),
            patch(
                "tongyonggongju.routes.tongyonggongju_routes.run_background_check",
                return_value=fake_result,
            ) as mock_check,
        ):
            response = self.client.post(
                "/tongyonggongju/api/background/check",
                json={"token": "abc123456789012345", "id_column_index": 2},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(mock_check.call_args.args, ("abc123456789012345", 2))


if __name__ == "__main__":
    unittest.main()
