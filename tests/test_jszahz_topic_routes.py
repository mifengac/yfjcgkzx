import io
import json
import unittest
from unittest.mock import patch

from flask import Flask

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp
from jszahzyj.service import jszahz_topic_service


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
            "managed_only": True,
            "branch_options": [{"value": "445302000000", "label": "云城分局"}],
            "person_type_options": [],
            "risk_options": [],
            "active_batches": {"base_batch": None, "tag_batch": None},
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

    def test_upload_base_uses_session_username(self) -> None:
        self._login()

        def _fake_stream(*, file_bytes, filename, created_by):
            yield {"progress": True, "title": "解析中", "api_version": jszahz_topic_service.UPLOAD_API_VERSION}
            yield {"success": True, "batch_id": 9, "api_version": jszahz_topic_service.UPLOAD_API_VERSION}

        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.import_jszahz_base_excel_stream",
            side_effect=_fake_stream,
        ) as mock_stream:
            response = self.client.post(
                "/jszahzyj/api/jszahzztk/upload_base",
                data={"file": (io.BytesIO(b"demo"), "demo.xlsx")},
                content_type="multipart/form-data",
            )

        lines = [
            json.loads(line)
            for line in response.data.decode("utf-8").strip().split("\n")
            if line.strip()
        ]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("X-JSZAHZ-Upload-Version"),
            jszahz_topic_service.UPLOAD_API_VERSION,
        )
        self.assertTrue(lines[-1]["success"])
        self.assertEqual(lines[-1]["api_version"], jszahz_topic_service.UPLOAD_API_VERSION)
        self.assertEqual(mock_stream.call_args.kwargs["filename"], "demo.xlsx")
        self.assertEqual(mock_stream.call_args.kwargs["created_by"], "tester")

    def test_upload_tags_uses_session_username(self) -> None:
        self._login()

        def _fake_stream(*, file_bytes, filename, created_by):
            yield {"progress": True, "title": "解析中", "api_version": jszahz_topic_service.UPLOAD_API_VERSION}
            yield {"success": True, "batch_id": 10, "api_version": jszahz_topic_service.UPLOAD_API_VERSION}

        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.import_jszahz_tag_excel_stream",
            side_effect=_fake_stream,
        ) as mock_stream:
            response = self.client.post(
                "/jszahzyj/api/jszahzztk/upload_tags",
                data={"file": (io.BytesIO(b"demo"), "tags.xlsx")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_stream.call_args.kwargs["filename"], "tags.xlsx")
        self.assertEqual(mock_stream.call_args.kwargs["created_by"], "tester")

    def test_query_returns_summary_payload(self) -> None:
        self._login()
        fake_payload = {
            "success": True,
            "records": [{"分局代码": "445302000000", "分局名称": "云城分局", "去重患者数": 2}],
            "count": 2,
            "message": "",
            "filters": {
                "branch_codes": [],
                "person_types": [],
                "risk_labels": [],
                "managed_only": True,
            },
            "active_batches": {"base_batch": None, "tag_batch": {"id": 3}},
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
                    "branch_codes": ["445302000000"],
                    "person_types": ["弱监护"],
                    "risk_labels": ["1级患者"],
                    "managed_only": True,
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
        ) as mock_export:
            response = self.client.get(
                "/jszahzyj/download/jszahzztk?"
                "&branch_codes=445302000000"
                "&person_types=%E5%BC%B1%E7%9B%91%E6%8A%A4"
                "&risk_labels=1%E7%BA%A7%E6%82%A3%E8%80%85"
                "&managed_only=1"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response.headers["Content-Disposition"])
        self.assertIn("summary.xlsx", response.headers["Content-Disposition"])
        self.assertTrue(mock_export.call_args.kwargs["managed_only"])


if __name__ == "__main__":
    unittest.main()
