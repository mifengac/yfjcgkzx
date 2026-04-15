import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from hqzcsj.routes.zfba_jq_aj_routes import zfba_jq_aj_bp


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


class TestZfbaJqAjRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(zfba_jq_aj_bp)

        @app.route("/login")
        def login():
            return "login"

        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_summary_requires_login(self) -> None:
        response = self.client.get("/zfba_jq_aj/api/summary")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_summary_uses_default_time_range_and_ratio_flag(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.zfba_jq_aj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.zfba_jq_aj_routes.default_time_range_for_page",
            return_value=("2026-04-01 00:00:00", "2026-04-08 00:00:00"),
        ), patch(
            "hqzcsj.routes.zfba_jq_aj_routes.build_summary",
            return_value=(
                SimpleNamespace(start_time="2026-04-01 00:00:00", end_time="2026-04-08 00:00:00"),
                [{"警情": 3, "环比警情": 1}],
            ),
        ) as mock_build_summary, patch(
            "hqzcsj.routes.zfba_jq_aj_routes.append_ratio_columns",
            return_value=[{"警情": 3, "环比警情比例": "200.00%"}],
        ) as mock_append_ratio:
            response = self.client.get("/zfba_jq_aj/api/summary?show_ratio=1&leixing=治安&za_type=行政")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["rows"][0]["警情"], 3)
        self.assertEqual(mock_build_summary.call_args.kwargs["start_time"], "2026-04-01 00:00:00")
        self.assertEqual(mock_build_summary.call_args.kwargs["end_time"], "2026-04-08 00:00:00")
        self.assertEqual(mock_build_summary.call_args.kwargs["leixing_list"], ["治安"])
        self.assertEqual(mock_build_summary.call_args.kwargs["za_types"], ["行政"])
        self.assertTrue(mock_append_ratio.called)

    def test_detail_page_renders_context_from_filters(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.zfba_jq_aj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.zfba_jq_aj_routes.fetch_detail",
            return_value=([{"姓名": "张三"}], True),
        ), patch(
            "hqzcsj.routes.zfba_jq_aj_routes.render_template",
            return_value="ok",
        ) as mock_render:
            response = self.client.get(
                "/zfba_jq_aj/detail"
                "?metric=%E8%AD%A6%E6%83%85"
                "&diqu=445302"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
                "&leixing=%E6%B2%BB%E5%AE%89"
                "&za_type=%E8%A1%8C%E6%94%BF"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_render.call_args.args[0], "zfba_jq_aj_detail.html")
        self.assertEqual(mock_render.call_args.kwargs["metric"], "警情")
        self.assertEqual(mock_render.call_args.kwargs["diqu"], "445302")
        self.assertEqual(mock_render.call_args.kwargs["region_name"], "云城")
        self.assertEqual(mock_render.call_args.kwargs["leixing_list"], ["治安"])
        self.assertEqual(mock_render.call_args.kwargs["za_types"], ["行政"])
        self.assertTrue(mock_render.call_args.kwargs["truncated"])

    def test_export_summary_preserves_first_row_header_order(self) -> None:
        self._login()
        with patch(
            "hqzcsj.routes.zfba_jq_aj_routes.get_database_connection",
            return_value=_DummyConnection((1,)),
        ), patch(
            "hqzcsj.routes.zfba_jq_aj_routes.build_summary",
            return_value=(SimpleNamespace(), [{"B列": 2, "A列": 1}]),
        ):
            response = self.client.get(
                "/zfba_jq_aj/export"
                "?fmt=csv"
                "&start_time=2026-04-01%2000:00:00"
                "&end_time=2026-04-08%2000:00:00"
            )

        body = response.data.decode("utf-8-sig")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response.headers["Content-Type"])
        self.assertTrue(body.startswith("B列,A列"))


if __name__ == "__main__":
    unittest.main()