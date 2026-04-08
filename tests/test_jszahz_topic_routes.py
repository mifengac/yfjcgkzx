import io
import unittest
from unittest.mock import patch

from flask import Flask

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp


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


class TestJszahzTopicRoutes(unittest.TestCase):
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

    def test_defaults_requires_login(self) -> None:
        response = self.client.get("/jszahzyj/api/jszahzztk/defaults")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_defaults_requires_permission(self) -> None:
        self._login()
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection(None),
        ):
            response = self.client.get("/jszahzyj/api/jszahzztk/defaults")

        self.assertEqual(response.status_code, 403)

    def test_defaults_returns_payload_for_authorized_user(self) -> None:
        self._login()
        fake_payload = {
            "success": True,
            "start_time": "2026-04-01 00:00:00",
            "end_time": "2026-04-08 00:00:00",
            "branch_options": [{"value": "445302000000", "label": "云城分局"}],
            "person_type_options": [],
            "risk_options": [],
            "active_batch": None,
        }
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.defaults_payload",
            return_value=fake_payload,
        ):
            response = self.client.get("/jszahzyj/api/jszahzztk/defaults")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["branch_options"][0]["value"], "445302000000")

    def test_upload_uses_session_username(self) -> None:
        self._login()
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.import_jszahz_topic_excel",
            return_value={"success": True, "batch_id": 9},
        ) as mock_import:
            response = self.client.post(
                "/jszahzyj/api/jszahzztk/upload",
                data={"file": (io.BytesIO(b"demo"), "demo.xlsx")},
                content_type="multipart/form-data",
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(mock_import.call_args.kwargs["filename"], "demo.xlsx")
        self.assertEqual(mock_import.call_args.kwargs["created_by"], "tester")

    def test_query_returns_summary_payload(self) -> None:
        self._login()
        fake_payload = {
            "success": True,
            "records": [{"分局代码": "445302000000", "分局名称": "云城分局", "去重患者数": 2}],
            "count": 2,
            "message": "",
            "filters": {
                "start_time": "2026-04-01 00:00:00",
                "end_time": "2026-04-08 00:00:00",
                "branch_codes": [],
                "person_types": [],
                "risk_labels": [],
            },
            "active_batch": {"id": 3},
        }
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.query_summary_payload",
            return_value=fake_payload,
        ) as mock_query:
            response = self.client.post(
                "/jszahzyj/api/jszahzztk/query",
                json={
                    "start_time": "2026-04-01 00:00:00",
                    "end_time": "2026-04-08 00:00:00",
                    "branch_codes": ["445302000000"],
                    "person_types": ["弱监护"],
                    "risk_labels": ["1级患者"],
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["count"], 2)
        self.assertEqual(mock_query.call_args.kwargs["person_types"], ["弱监护"])

    def test_download_returns_xlsx_attachment(self) -> None:
        self._login()
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.export_summary_xlsx",
            return_value=(b"xlsx-bytes", "summary.xlsx"),
        ):
            response = self.client.get(
                "/jszahzyj/download/jszahzztk"
                "?start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
                "&branch_codes=445302000000"
                "&person_types=%E5%BC%B1%E7%9B%91%E6%8A%A4"
                "&risk_labels=1%E7%BA%A7%E6%82%A3%E8%80%85"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response.headers["Content-Disposition"])
        self.assertIn("summary.xlsx", response.headers["Content-Disposition"])


if __name__ == "__main__":
    unittest.main()
