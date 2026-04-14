import json
import unittest
from unittest.mock import patch

from flask import Flask

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp


class _DummyCursor:
    def __init__(self, row=None, *, raise_on_execute: Exception | None = None):
        self._row = row
        self._raise_on_execute = raise_on_execute

    def execute(self, _sql, _params=None) -> None:
        if self._raise_on_execute is not None:
            raise self._raise_on_execute

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConnection:
    def __init__(self, row=None, *, raise_on_execute: Exception | None = None):
        self._row = row
        self._raise_on_execute = raise_on_execute
        self.closed = False

    def cursor(self):
        return _DummyCursor(self._row, raise_on_execute=self._raise_on_execute)

    def close(self) -> None:
        self.closed = True


class TestJszahzyjRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jszahzyj_bp, url_prefix="/jszahzyj")

        @app.route("/login")
        def login():
            return "login"

        self.app = app
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_index_invalid_page_uses_defaults(self) -> None:
        self._login()
        render_calls = []

        def _fake_render(_template, **context):
            render_calls.append(context)
            return json.dumps(context, ensure_ascii=False)

        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahzyj_routes.get_jszahzyj_data",
            return_value=([], 0),
        ), patch(
            "jszahzyj.routes.jszahzyj_routes.render_template",
            side_effect=_fake_render,
        ):
            response = self.client.get("/jszahzyj/?page=abc&page_size=0")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(render_calls[0]["page"], 1)
        self.assertEqual(render_calls[0]["page_size"], 1)

    def test_access_check_closes_connection_on_error(self) -> None:
        self._login()
        conn = _DummyConnection(raise_on_execute=RuntimeError("db error"))
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=conn,
        ):
            response = self.client.get("/jszahzyj/")

        self.assertEqual(response.status_code, 500)
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()