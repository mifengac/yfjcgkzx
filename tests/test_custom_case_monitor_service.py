import unittest
from io import BytesIO
from unittest.mock import patch

from openpyxl import load_workbook

from jingqing_fenxi.service import custom_case_monitor_service as monitor_service
from jingqing_fenxi.service import special_case_tab_service as special_service


class TestCustomCaseMonitorService(unittest.TestCase):
    def test_validate_rules_accepts_multiline_values(self) -> None:
        rules = special_service.validate_scheme_rules(
            [
                {
                    "field_name": "combined_text",
                    "operator": "contains_any",
                    "rule_values": "流浪\n乞讨",
                }
            ]
        )

        self.assertEqual(rules[0]["rule_values"], ["流浪", "乞讨"])
        self.assertTrue(rules[0]["is_enabled"])

    def test_validate_rules_rejects_empty_values(self) -> None:
        with self.assertRaises(ValueError):
            special_service.validate_scheme_rules(
                [
                    {
                        "field_name": "combined_text",
                        "operator": "contains_any",
                        "rule_values": "",
                    }
                ]
            )

    def test_query_custom_case_monitor_records_matches_seeded_logic(self) -> None:
        rows = [
            {
                "caseNo": "A001",
                "cmdId": "445300000000",
                "caseContents": "发现流浪人员求助",
                "replies": "",
                "callTime": "2026-04-05 08:00:00",
                "dutyDeptName": "测试派出所",
            },
            {
                "caseNo": "A002",
                "cmdId": "445300000000",
                "caseContents": "普通警情",
                "replies": "现场涉及乞讨人员聚集",
                "callTime": "2026-04-05 09:00:00",
                "dutyDeptName": "测试派出所",
            },
            {
                "caseNo": "A003",
                "cmdId": "445300000000",
                "caseContents": "普通纠纷",
                "replies": "",
                "callTime": "2026-04-05 10:00:00",
                "dutyDeptName": "测试派出所",
            },
        ]
        scheme = {
            "id": 1,
            "scheme_name": "流浪/乞讨警情",
            "is_enabled": True,
            "rules": [
                {
                    "field_name": "combined_text",
                    "operator": "contains_any",
                    "rule_values": ["流浪", "乞讨"],
                    "is_enabled": True,
                }
            ],
        }

        with patch.object(monitor_service.dao, "get_scheme_by_id", return_value=scheme), patch.object(
            special_service,
            "fetch_all_case_list",
            return_value=rows,
        ):
            result = monitor_service.query_custom_case_monitor_records(
                scheme_id=1,
                start_time="2026-04-05 00:00:00",
                end_time="2026-04-05 23:59:59",
                branches=[],
                page_num=1,
                page_size=15,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["scheme_name"], "流浪/乞讨警情")

    def test_query_rejects_disabled_scheme(self) -> None:
        with patch.object(monitor_service.dao, "get_scheme_by_id", return_value={"id": 1, "is_enabled": False}):
            with self.assertRaises(ValueError):
                monitor_service.query_custom_case_monitor_records(
                    scheme_id=1,
                    start_time="2026-04-05 00:00:00",
                    end_time="2026-04-05 23:59:59",
                    branches=[],
                    page_num=1,
                    page_size=15,
                )

    def test_export_xlsx_adds_hit_keywords_and_safe_filename(self) -> None:
        rules = [
            {
                "field_name": "combined_text",
                "operator": "contains_any",
                "rule_values": ["坟地", "林地", "纠纷"],
                "is_enabled": True,
            }
        ]
        rows = [
            {
                "caseNo": "A001",
                "callTime": "2026-04-05 08:00:00",
                "cmdId": "445300000000",
                "dutyDeptName": "测试派出所",
                "caseLevelName": "一级",
                "occurAddress": "测试地址",
                "callerName": "张三",
                "callerPhone": "13800138000",
                "caseContents": "坟地纠纷",
                "replies": "林地争执",
            }
        ]

        with patch.object(monitor_service.dao, "get_scheme_by_id", return_value={
            "id": 1,
            "scheme_name": "流浪/乞讨警情",
            "is_enabled": True,
            "rules": rules,
        }), patch.object(special_service, "fetch_all_case_list", return_value=rows):
            buffer, mimetype, filename = monitor_service.export_custom_case_monitor_records(
                export_format="xlsx",
                scheme_id=1,
                start_time="2026-04-05 00:00:00",
                end_time="2026-04-05 23:59:59",
                branches=[],
            )

        workbook = load_workbook(BytesIO(buffer.getvalue()))
        worksheet = workbook.active
        headers = [cell.value for cell in worksheet[1]]

        self.assertEqual(mimetype, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.assertEqual(filename, "流浪_乞讨警情_2026-04-05_2026-04-05.xlsx")
        self.assertEqual(worksheet.title, "流浪_乞讨警情")
        self.assertIn("命中关键字", headers)
        self.assertEqual(worksheet.cell(row=2, column=len(headers)).value, "坟地、林地、纠纷")

    def test_export_csv_keeps_columns_and_uses_safe_filename(self) -> None:
        rules = [
            {
                "field_name": "combined_text",
                "operator": "contains_any",
                "rule_values": ["坟地", "林地", "纠纷"],
                "is_enabled": True,
            }
        ]
        rows = [
            {
                "caseNo": "A001",
                "callTime": "2026-04-05 08:00:00",
                "cmdId": "445300000000",
                "dutyDeptName": "测试派出所",
                "caseLevelName": "一级",
                "occurAddress": "测试地址",
                "callerName": "张三",
                "callerPhone": "13800138000",
                "caseContents": "坟地纠纷",
                "replies": "林地争执",
            }
        ]

        with patch.object(monitor_service.dao, "get_scheme_by_id", return_value={
            "id": 1,
            "scheme_name": "流浪/乞讨警情",
            "is_enabled": True,
            "rules": rules,
        }), patch.object(special_service, "fetch_all_case_list", return_value=rows):
            buffer, mimetype, filename = monitor_service.export_custom_case_monitor_records(
                export_format="csv",
                scheme_id=1,
                start_time="2026-04-05 00:00:00",
                end_time="2026-04-05 23:59:59",
                branches=[],
            )

        csv_text = buffer.getvalue().decode("utf-8-sig")
        header_line = csv_text.splitlines()[0]

        self.assertEqual(mimetype, "text/csv; charset=utf-8")
        self.assertEqual(filename, "流浪_乞讨警情_2026-04-05_2026-04-05.csv")
        self.assertNotIn("命中关键字", header_line)


if __name__ == "__main__":
    unittest.main()
