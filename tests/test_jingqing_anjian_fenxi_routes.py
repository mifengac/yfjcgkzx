import unittest
from flask import Flask
from unittest.mock import patch

from jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes import jingqing_anjian_fenxi_bp
from jingqing_anjian_fenxi.service.jingqing_anjian_fenxi_service import SummaryMeta


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


class TestJingqingAnjianFenxiRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jingqing_anjian_fenxi_bp, url_prefix="/jingqing_anjian_fenxi")

        @app.route("/login")
        def login():
            return "login"

        self.app = app
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_requires_login_redirects_to_login(self) -> None:
        response = self.client.get("/jingqing_anjian_fenxi/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_requires_permission_returns_403(self) -> None:
        self._login()
        with patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.get_database_connection",
            return_value=_DummyConnection(None),
        ):
            response = self.client.get("/jingqing_anjian_fenxi/")
        self.assertEqual(response.status_code, 403)

    def test_api_summary_returns_json_for_authorized_user(self) -> None:
        self._login()
        meta = SummaryMeta(
            start_time="2026-03-22 00:00:00",
            end_time="2026-03-29 00:00:00",
            group_mode="county",
            group_mode_label="县市区",
        )
        rows = [{"分局": "全市", "当前分组名称": "全市", "及时立案平均小时": "4.8"}]
        with patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.jingqing_anjian_fenxi_service.build_summary",
            return_value=(meta, rows),
        ):
            response = self.client.get("/jingqing_anjian_fenxi/api/summary?group_mode=county")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["meta"]["group_mode_label"], "县市区")
        self.assertEqual(payload["rows"][0]["当前分组名称"], "全市")

    def test_detail_page_renders_for_authorized_user(self) -> None:
        self._login()
        with patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.jingqing_anjian_fenxi_service.fetch_detail",
            return_value=([{"分局": "全市", "当前分组名称": "全市", "时差(小时)": "4.8"}], False),
        ):
            response = self.client.get(
                "/jingqing_anjian_fenxi/detail"
                "?metric=timely_filing&group_code=__ALL__&group_name=全市"
                "&start_time=2026-03-22%2000:00:00&end_time=2026-03-29%2000:00:00"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("全市 - 及时立案平均小时", response.get_data(as_text=True))

    def test_export_summary_returns_csv(self) -> None:
        self._login()
        meta = SummaryMeta(
            start_time="2026-03-22 00:00:00",
            end_time="2026-03-29 00:00:00",
            group_mode="station",
            group_mode_label="派出所",
        )
        rows = [
            {
                "分局": "全市",
                "当前分组名称": "全市",
                "及时立案平均小时": "4.8",
                "及时研判抓人平均小时": "",
                "及时破案平均小时": "",
                "及时结案平均小时": "",
            }
        ]
        with patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.jingqing_anjian_fenxi_service.build_summary",
            return_value=(meta, rows),
        ):
            response = self.client.get(
                "/jingqing_anjian_fenxi/export"
                "?fmt=csv&group_mode=station"
                "&start_time=2026-03-22%2000:00:00&end_time=2026-03-29%2000:00:00"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.content_type)
        self.assertIn("attachment;", response.headers["Content-Disposition"])

    def test_detail_export_returns_csv(self) -> None:
        self._login()
        with patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "jingqing_anjian_fenxi.routes.jingqing_anjian_fenxi_routes.jingqing_anjian_fenxi_service.fetch_detail",
            return_value=([{"分局": "全市", "时差(小时)": "4.8"}], False),
        ):
            response = self.client.get(
                "/jingqing_anjian_fenxi/detail/export"
                "?fmt=csv&metric=timely_filing&group_code=__ALL__&group_name=全市"
                "&start_time=2026-03-22%2000:00:00&end_time=2026-03-29%2000:00:00"
            )

        body = response.get_data().decode("utf-8-sig")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.content_type)
        self.assertIn("分局", body)


if __name__ == "__main__":
    unittest.main()
