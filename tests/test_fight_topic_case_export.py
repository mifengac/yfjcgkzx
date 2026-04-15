import io
import unittest
from datetime import datetime as real_datetime
from unittest.mock import patch

from flask import Flask
from openpyxl import load_workbook

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service import fight_topic_case_export_service as export_service


class TestFightTopicCaseExportService(unittest.TestCase):
    def test_generate_unclosed_fight_cases_excel_writes_summary_row_headers_and_data(self) -> None:
        buffer = export_service.generate_unclosed_fight_cases_excel(
            [
                {
                    "ay_name": "故意伤害",
                    "case_name": "张三李四打架案",
                    "case_status": "立案",
                    "handling_unit": "城中派出所",
                    "filing_time": "2026-04-10 10:00:00",
                    "incident_address": "云城区建设路",
                    "incident_time": "2026-04-09 23:00:00",
                    "summary": "双方酒后发生冲突",
                }
            ],
            begin_date="2026-04-01 00:00:00",
            end_date="2026-04-10 23:59:59",
        )

        workbook = load_workbook(io.BytesIO(buffer.getvalue()))
        sheet = workbook.active

        self.assertEqual(sheet["A1"].value, "统计时间:2026-04-01 00:00:00-2026-04-10 23:59:59")
        self.assertEqual(sheet["A2"].value, "案由")
        self.assertEqual(sheet["B2"].value, "案件名称")
        self.assertEqual(sheet["A3"].value, "故意伤害")
        self.assertEqual(sheet["H3"].value, "双方酒后发生冲突")

    def test_export_unclosed_fight_cases_queries_rows_by_time_range(self) -> None:
        with patch.object(
            export_service,
            "list_unclosed_fight_cases",
            return_value=[],
        ) as mock_list:
            export_service.export_unclosed_fight_cases(
                {"beginDate": "2026-04-01 00:00:00", "endDate": "2026-04-10 23:59:59"}
            )

        mock_list.assert_called_once_with("2026-04-01 00:00:00", "2026-04-10 23:59:59")

    def test_build_unclosed_case_export_filename_matches_expected_format(self) -> None:
        filename = export_service.build_unclosed_case_export_filename(
            "2026-04-01 00:00:00",
            "2026-04-10 23:59:59",
            now=real_datetime(2026, 4, 15, 8, 9, 10),
        )

        self.assertEqual(filename, "2026-04-01-2026-04-10打架斗殴未办结案件明细20260415080910.xlsx")


class TestFightTopicCaseExportRoute(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jingqing_fenxi_bp, url_prefix="/jingqing_fenxi")
        self.client = app.test_client()

    def test_download_unclosed_cases_route_returns_workbook(self) -> None:
        with patch(
            "jingqing_fenxi.routes.fight_topic_routes.export_unclosed_fight_cases",
            return_value=(io.BytesIO(b"test-export"), {"beginDate": "2026-04-01 00:00:00", "endDate": "2026-04-10 23:59:59"}),
        ) as mock_export, patch(
            "jingqing_fenxi.routes.fight_topic_routes.build_unclosed_case_export_filename",
            return_value="fight-unclosed.xlsx",
        ):
            response = self.client.get(
                "/jingqing_fenxi/download/fight-topic/unclosed-cases?beginDate=2026-04-01 00:00:00&endDate=2026-04-10 23:59:59"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"test-export")
        self.assertEqual(mock_export.call_args.args[0]["beginDate"], "2026-04-01 00:00:00")
        self.assertEqual(mock_export.call_args.args[0]["endDate"], "2026-04-10 23:59:59")
        self.assertIn("fight-unclosed.xlsx", response.headers["Content-Disposition"])


if __name__ == "__main__":
    unittest.main()