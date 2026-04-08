import unittest
from unittest.mock import patch

from flask import Flask

from zhizong.routes.zhizong_routes import zhizong_bp


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


class TestZhizongRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(zhizong_bp, url_prefix="/zhizong")

        @app.route("/login")
        def login():
            return "login"

        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_index_requires_login(self) -> None:
        response = self.client.get("/zhizong/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_index_requires_permission(self) -> None:
        self._login()
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection(None),
        ):
            response = self.client.get("/zhizong/")

        self.assertEqual(response.status_code, 403)

    def test_task_detail_renders_summary_context(self) -> None:
        self._login()
        fake_task = {"id": 1, "task_name": "任务A", "table_name": "task_table"}
        fake_rows = [{"地区": "云城", "数量": 2}]
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "zhizong.routes.zhizong_routes.get_task_metadata",
            return_value=fake_task,
        ), patch(
            "zhizong.routes.zhizong_routes.fetch_task_summary",
            return_value=fake_rows,
        ), patch(
            "zhizong.routes.zhizong_routes.render_template",
            return_value="ok",
        ) as mock_render:
            response = self.client.get("/zhizong/task/1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_render.call_args.args[0], "zhizong_detail.html")
        self.assertEqual(mock_render.call_args.kwargs["task"], fake_task)
        self.assertEqual(mock_render.call_args.kwargs["summary_rows"], fake_rows)

    def test_region_detail_returns_json_payload(self) -> None:
        self._login()
        fake_task = {"id": 1, "task_name": "任务A", "table_name": "task_table"}
        fake_rows = [{"姓名": "张三"}]
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "zhizong.routes.zhizong_routes.get_task_metadata",
            return_value=fake_task,
        ), patch(
            "zhizong.routes.zhizong_routes.fetch_task_detail_rows",
            return_value=fake_rows,
        ):
            response = self.client.get("/zhizong/api/task/1/region_detail?dwdm=445302")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"], fake_rows)

    def test_task_rows_page_uses_region_title(self) -> None:
        self._login()
        fake_task = {"id": 1, "task_name": "任务A", "table_name": "task_table"}
        fake_rows = [{"姓名": "张三"}]
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "zhizong.routes.zhizong_routes.get_task_metadata",
            return_value=fake_task,
        ), patch(
            "zhizong.routes.zhizong_routes.fetch_task_detail_rows",
            return_value=fake_rows,
        ), patch(
            "zhizong.routes.zhizong_routes.render_template",
            return_value="ok",
        ) as mock_render:
            response = self.client.get("/zhizong/task/1/rows?dwdm=445302")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_render.call_args.args[0], "zhizong_rows.html")
        self.assertEqual(mock_render.call_args.kwargs["title"], "任务A - 云城")
        self.assertEqual(mock_render.call_args.kwargs["rows"], fake_rows)

    def test_download_home_summary_returns_excel(self) -> None:
        self._login()
        fake_rows = [{"任务名": "任务A", "总计": 2}]
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "zhizong.routes.zhizong_routes.fetch_home_summary",
            return_value=fake_rows,
        ):
            response = self.client.get("/zhizong/download/home_summary")

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.headers["Content-Type"],
        )
        self.assertIn("attachment;", response.headers["Content-Disposition"])

    def test_task_rows_download_returns_csv(self) -> None:
        self._login()
        fake_task = {"id": 1, "task_name": "任务A", "table_name": "task_table"}
        fake_rows = [{"姓名": "张三", "编号": "001"}]
        with patch(
            "zhizong.routes.zhizong_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "zhizong.routes.zhizong_routes.get_task_metadata",
            return_value=fake_task,
        ), patch(
            "zhizong.routes.zhizong_routes.fetch_task_rows_all",
            return_value=fake_rows,
        ):
            response = self.client.get("/zhizong/task/1/rows/download")

        body = response.data.decode("utf-8-sig")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.headers["Content-Type"])
        self.assertIn("姓名", body)
        self.assertIn("张三", body)


if __name__ == "__main__":
    unittest.main()
