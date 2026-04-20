import unittest
from io import BytesIO
from unittest.mock import patch

from flask import Flask

from xunfang.routes.xunfang_routes import xunfang_bp


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


class TestJiemiansanleiRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(xunfang_bp, url_prefix="/xunfang")

        @app.route("/login")
        def login():
            return "login"

        self.app = app
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_query_forwards_street_and_minor_filters(self) -> None:
        self._login()
        with patch(
            "xunfang.routes.xunfang_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xunfang.routes.jiemiansanlei_routes.query_classified",
            return_value={"total": 1, "page": 1, "page_size": 20, "rows": []},
        ) as mock_query:
            response = self.client.post(
                "/xunfang/jiemiansanlei/query",
                json={
                    "startTime": "2026-03-01 00:00:00",
                    "endTime": "2026-03-02 00:00:00",
                    "leixingList": ["盗窃"],
                    "yuanshiquerenList": ["原始"],
                    "page": 1,
                    "pageSize": 20,
                    "streetOnly": True,
                    "minorOnly": True,
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertTrue(mock_query.call_args.kwargs["street_only"])
        self.assertEqual(mock_query.call_args.kwargs["street_filter_mode"], "model")
        self.assertTrue(mock_query.call_args.kwargs["minor_only"])

    def test_export_forwards_street_and_minor_filters(self) -> None:
        self._login()
        with patch(
            "xunfang.routes.xunfang_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xunfang.routes.jiemiansanlei_routes.export_classified",
            return_value=(b"test", "application/vnd.ms-excel", "street.xls"),
        ) as mock_export:
            response = self.client.post(
                "/xunfang/jiemiansanlei/export?format=xls",
                json={
                    "startTime": "2026-03-01 00:00:00",
                    "endTime": "2026-03-02 00:00:00",
                    "leixingList": ["盗窃"],
                    "yuanshiquerenList": ["原始"],
                    "streetOnly": True,
                    "minorOnly": True,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response.headers["Content-Disposition"])
        self.assertTrue(mock_export.call_args.kwargs["street_only"])
        self.assertEqual(mock_export.call_args.kwargs["street_filter_mode"], "model")
        self.assertTrue(mock_export.call_args.kwargs["minor_only"])

    def test_query_forwards_dropdown_street_filter_mode(self) -> None:
        self._login()
        with patch(
            "xunfang.routes.xunfang_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xunfang.routes.jiemiansanlei_routes.query_classified",
            return_value={"total": 1, "page": 1, "page_size": 20, "rows": []},
        ) as mock_query:
            response = self.client.post(
                "/xunfang/jiemiansanlei/query",
                json={
                    "startTime": "2026-03-01 00:00:00",
                    "endTime": "2026-03-02 00:00:00",
                    "leixingList": ["盗窃"],
                    "yuanshiquerenList": ["原始"],
                    "page": 1,
                    "pageSize": 20,
                    "streetFilterMode": "content_road",
                    "minorOnly": False,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(mock_query.call_args.kwargs["street_only"])
        self.assertEqual(mock_query.call_args.kwargs["street_filter_mode"], "content_road")

    def test_export_report_forwards_time_and_street_filter_mode(self) -> None:
        self._login()
        with patch(
            "xunfang.routes.xunfang_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xunfang.routes.jiemiansanlei_routes.export_report",
            return_value=(
                BytesIO(b"report").getvalue(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "report.xlsx",
            ),
        ) as mock_export:
            response = self.client.post(
                "/xunfang/jiemiansanlei/export_report",
                json={
                    "startTime": "2026-03-01 00:00:00",
                    "endTime": "2026-03-02 00:00:00",
                    "hbStartTime": "2026-02-22 00:00:00",
                    "hbEndTime": "2026-02-23 00:00:00",
                    "streetFilterMode": "reply_public",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_export.call_args.kwargs,
            {
                "start_time": "2026-03-01 00:00:00",
                "end_time": "2026-03-02 00:00:00",
                "hb_start_time": "2026-02-22 00:00:00",
                "hb_end_time": "2026-02-23 00:00:00",
                "street_filter_mode": "reply_public",
            },
        )


if __name__ == "__main__":
    unittest.main()
