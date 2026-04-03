import io
import unittest
from unittest.mock import patch

from flask import Flask
from openpyxl import load_workbook

from mdjfxsyj.service import mdjfxsyj_yyjdjc_service as service


class _FixedDatetime(service.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 4, 3, 15, 30, 0)


class TestMdjfxsyjYyjdjcService(unittest.TestCase):
    def test_find_matched_keywords_keeps_config_order(self) -> None:
        matched = service.find_matched_keywords(
            ["有人扬言报复社会，还说要放火并且再次扬言"],
            ["扬言", "放火", "报复社会", "扬言"],
        )

        self.assertEqual(matched, ["扬言", "放火", "报复社会"])

    def test_default_range_uses_today_midnight_and_minus_eight_days(self) -> None:
        with patch.object(service, "datetime", _FixedDatetime):
            start_dt, end_dt = service.default_range()

        self.assertEqual(start_dt.strftime("%Y-%m-%d %H:%M:%S"), "2026-03-26 00:00:00")
        self.assertEqual(end_dt.strftime("%Y-%m-%d %H:%M:%S"), "2026-04-03 00:00:00")

    def test_get_monitor_data_collects_source_errors_individually(self) -> None:
        with patch.object(service, "fetch_police_rows", side_effect=RuntimeError("警情接口不可用")), \
             patch.object(service, "fetch_workorder_source_rows", return_value=[{"业务编号": "A1", "命中关键词": "扬言"}]), \
             patch.object(service, "fetch_dispute_source_rows", return_value=[]):
            payload = service.get_monitor_data(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-03 00:00:00",
            )

        self.assertEqual(payload["sources"]["police"]["count"], 0)
        self.assertIn("警情接口不可用", payload["sources"]["police"]["error"])
        self.assertEqual(payload["sources"]["workorder"]["count"], 1)
        self.assertEqual(payload["sources"]["dispute"]["rows"], [])

    def test_build_all_sources_export_has_three_fixed_sheets(self) -> None:
        fake_payload = {
            "start_time": "2026-04-01 00:00:00",
            "end_time": "2026-04-03 00:00:00",
            "keywords": list(service.DEFAULT_KEYWORDS),
            "sources": {
                "police": {
                    "rows": [{"警情编号": "J1", "命中关键词": "扬言"}],
                    "columns": service.SOURCE_SPECS["police"]["columns"],
                    "count": 1,
                    "error": "",
                },
                "workorder": {
                    "rows": [],
                    "columns": service.SOURCE_SPECS["workorder"]["columns"],
                    "count": 0,
                    "error": "",
                },
                "dispute": {
                    "rows": [],
                    "columns": service.SOURCE_SPECS["dispute"]["columns"],
                    "count": 0,
                    "error": "测试错误",
                },
            },
        }

        app = Flask(__name__)
        with app.test_request_context("/"), patch.object(service, "get_monitor_data", return_value=fake_payload):
            response = service.build_all_sources_export(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-03 00:00:00",
            )
            response.direct_passthrough = False
            workbook = load_workbook(io.BytesIO(response.get_data()))

        self.assertEqual(
            workbook.sheetnames,
            [
                service.SOURCE_SPECS["police"]["sheet_name"],
                service.SOURCE_SPECS["workorder"]["sheet_name"],
                service.SOURCE_SPECS["dispute"]["sheet_name"],
            ],
        )
        self.assertEqual(workbook[service.SOURCE_SPECS["police"]["sheet_name"]]["A2"].value, "J1")
        self.assertEqual(workbook[service.SOURCE_SPECS["dispute"]["sheet_name"]]["A1"].value, "错误信息")


if __name__ == "__main__":
    unittest.main()
