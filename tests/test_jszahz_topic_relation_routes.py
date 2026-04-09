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


class TestJszahzTopicRelationRoutes(unittest.TestCase):
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

    def test_relation_page_requires_login(self) -> None:
        response = self.client.get("/jszahzyj/jszahzztk/relation_page?relation_type=case&zjhm=1")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_relation_page_requires_permission(self) -> None:
        self._login()
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection(None),
        ):
            response = self.client.get("/jszahzyj/jszahzztk/relation_page?relation_type=case&zjhm=1")

        self.assertEqual(response.status_code, 403)

    def test_relation_page_rejects_invalid_type(self) -> None:
        self._login()
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_relation_routes_impl.build_relation_page_payload",
            side_effect=ValueError("不支持的关联类型"),
        ):
            response = self.client.get("/jszahzyj/jszahzztk/relation_page?relation_type=bad&zjhm=1")

        self.assertEqual(response.status_code, 400)
        self.assertIn("不支持的关联类型", response.get_data(as_text=True))

    def test_relation_page_renders_empty_state(self) -> None:
        self._login()
        fake_payload = {
            "title": "关联案件明细",
            "xm": "张三",
            "zjhm": "440123199001011111",
            "records": [],
            "message": "未查询到该人员的关联案件数据",
        }
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_relation_routes_impl.build_relation_page_payload",
            return_value=fake_payload,
        ):
            response = self.client.get(
                "/jszahzyj/jszahzztk/relation_page"
                "?relation_type=case&zjhm=440123199001011111&xm=%E5%BC%A0%E4%B8%89"
            )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("关联案件明细", body)
        self.assertIn("张三", body)
        self.assertIn("未查询到该人员的关联案件数据", body)

    def test_relation_page_supports_racing(self) -> None:
        self._login()
        fake_payload = {
            "title": "关联飙车炸街明细",
            "xm": "张三",
            "zjhm": "440123199001011111",
            "records": [{"文书编号": "WS001"}],
            "message": "",
        }
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_relation_routes_impl.build_relation_page_payload",
            return_value=fake_payload,
        ):
            response = self.client.get(
                "/jszahzyj/jszahzztk/relation_page"
                "?relation_type=racing&zjhm=440123199001011111&xm=%E5%BC%A0%E4%B8%89"
            )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("关联飙车炸街明细", body)
        self.assertIn("WS001", body)

    def test_detail_page_renders_relation_links(self) -> None:
        self._login()
        fake_payload = {
            "filters": {
                "start_time": "2026-04-01 00:00:00",
                "end_time": "2026-04-08 00:00:00",
                "person_types": [],
                "risk_labels": [],
            },
            "records": [
                {
                    "姓名": "张三",
                    "身份证号": "440123199001011111",
                    "列管时间": "2026-04-01 08:00:00",
                    "列管单位": "云城派出所",
                    "分局": "云城分局",
                    "人员风险": "1级患者",
                    "人员类型": "弱监护",
                    "关联案件": 0,
                    "关联警情": 1,
                    "关联机动车": 2,
                    "关联视频云": 3,
                    "关联门诊": 4,
                    "关联飙车炸街": 5,
                }
            ],
            "message": "",
        }
        with patch(
            "jszahzyj.routes.jszahzyj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jszahzyj.routes.jszahz_topic_routes_impl.query_detail_payload",
            return_value=fake_payload,
        ):
            response = self.client.get(
                "/jszahzyj/jszahzztk/detail_page"
                "?branch_code=445302000000&branch_name=%E4%BA%91%E5%9F%8E%E5%88%86%E5%B1%80"
                "&start_time=2026-04-01%2000:00:00&end_time=2026-04-08%2000:00:00"
            )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("查看(0)", body)
        self.assertIn("查看(5)", body)
        self.assertIn("relation_type=case", body)
        self.assertIn("relation_type=clinic", body)
        self.assertIn("relation_type=racing", body)


if __name__ == "__main__":
    unittest.main()
