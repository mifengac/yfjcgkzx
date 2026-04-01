import unittest
from flask import Flask
from unittest.mock import patch

from xxffmk.routes.xxffmk_routes import xxffmk_bp


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


class TestXxffmkRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(xxffmk_bp, url_prefix="/xxffmk")

        @app.route("/login")
        def login():
            return "login"

        self.app = app
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_requires_login_redirects_to_login(self) -> None:
        response = self.client.get("/xxffmk/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_requires_permission_returns_403(self) -> None:
        self._login()
        with patch(
            "xxffmk.routes.xxffmk_routes.get_database_connection",
            return_value=_DummyConnection(None),
        ):
            response = self.client.get("/xxffmk/")
        self.assertEqual(response.status_code, 403)

    def test_api_rank_returns_payload_for_authorized_user(self) -> None:
        self._login()
        fake_payload = {
            "filters": {"beginDate": "2026-01-01 00:00:00", "endDate": "2026-03-31 23:59:59", "limit": 10},
            "rows": [{"rank": 1, "xxbsm": "A001", "xxmc": "甲学校", "total_score": 30, "dimension_scores": {}}],
            "total": 1,
            "dimension_order": [],
            "unmatched_summary": {},
        }
        with patch(
            "xxffmk.routes.xxffmk_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xxffmk.routes.xxffmk_routes.xxffmk_service.build_rank_payload",
            return_value=fake_payload,
        ):
            response = self.client.post(
                "/xxffmk/api/rank",
                json={"beginDate": "2026-01-01T00:00:00", "endDate": "2026-03-31T23:59:59", "limit": 10},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["rows"][0]["xxmc"], "甲学校")

    def test_api_refresh_returns_payload_for_authorized_user(self) -> None:
        self._login()
        fake_payload = {
            "refreshed_views": [
                '"ywdata"."mv_xxffmk_school_dim"',
                '"ywdata"."mv_xxffmk_student_school_rel"',
                '"ywdata"."mv_xxffmk_dim5_night_day"',
            ],
            "refreshed_count": 3,
            "elapsed_seconds": 0.123,
            "message": "已刷新 3 个物化视图",
        }
        with patch(
            "xxffmk.routes.xxffmk_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xxffmk.routes.xxffmk_routes.xxffmk_service.refresh_materialized_views",
            return_value=fake_payload,
        ):
            response = self.client.post("/xxffmk/api/refresh")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["refreshed_count"], 3)

    def test_school_detail_returns_404_when_missing(self) -> None:
        self._login()
        with patch(
            "xxffmk.routes.xxffmk_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xxffmk.routes.xxffmk_routes.xxffmk_service.get_school_detail",
            side_effect=LookupError("未找到对应学校"),
        ):
            response = self.client.get("/xxffmk/api/school_detail?xxbsm=NOT_FOUND")

        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.get_json()["success"])

    def test_dimension_detail_returns_payload(self) -> None:
        self._login()
        fake_payload = {
            "dimension": "涉校警情",
            "dimension_key": "jingqing",
            "columns": ["案件编号"],
            "rows": [{"案件编号": "JQ001"}],
            "total": 1,
            "page": 1,
            "page_size": 20,
            "unmatched_summary": [],
        }
        with patch(
            "xxffmk.routes.xxffmk_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "xxffmk.routes.xxffmk_routes.xxffmk_service.get_dimension_detail",
            return_value=fake_payload,
        ):
            response = self.client.get("/xxffmk/api/dimension_detail?dimension=jingqing&xxbsm=A001")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["rows"][0]["案件编号"], "JQ001")


if __name__ == "__main__":
    unittest.main()
