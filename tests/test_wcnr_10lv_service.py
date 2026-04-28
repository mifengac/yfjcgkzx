import unittest
from unittest.mock import MagicMock, patch

from hqzcsj.service.wcnr_10lv_service import (
    build_detail_export_sheets,
    build_campus_bullying_incident_case_export_rows,
    build_campus_bullying_export_title,
    get_display_columns,
    metric_display_name,
)


class TestWcnr10lvService(unittest.TestCase):
    def test_metric_display_name_supports_place_metrics(self) -> None:
        self.assertEqual(metric_display_name("jq_changsuo"), "警情(场所)")
        self.assertEqual(metric_display_name("aj_changsuo"), "案件(场所)")

    def test_display_columns_include_place_metrics(self) -> None:
        columns = get_display_columns(show_hb=True, show_ratio=True)

        self.assertIn("警情(场所)", columns)
        self.assertIn("案件(场所)", columns)
        self.assertGreater(columns.index("警情(场所)"), columns.index("转案率"))
        self.assertGreater(columns.index("案件(场所)"), columns.index("违法犯罪人员"))

    def test_detail_export_sheet_names_include_place_metrics(self) -> None:
        fake_conn = MagicMock()
        period_payload = {"counts": {}, "details": {}, "flags": {}}

        with patch(
            "hqzcsj.service.wcnr_10lv_service.get_database_connection",
            return_value=fake_conn,
        ), patch(
            "hqzcsj.service.wcnr_10lv_service.wcnr_10lv_dao.fetch_period_data",
            return_value=period_payload,
        ) as mock_fetch_period_data:
            sheets = build_detail_export_sheets(
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                hb_start_time=None,
                hb_end_time=None,
                leixing_list=[],
                show_hb=False,
            )

        sheet_names = [sheet["name"] for sheet in sheets]
        self.assertIn("警情(场所)-当前", sheet_names)
        self.assertIn("案件(场所)-当前", sheet_names)
        self.assertEqual(mock_fetch_period_data.call_count, 2)
        fake_conn.close.assert_called_once()

    def test_campus_bullying_export_rows_include_all_incidents_and_matched_cases(self) -> None:
        fake_conn = MagicMock()

        with patch(
            "hqzcsj.service.wcnr_10lv_service.wcnr_case_list_dao.fetch_campus_bullying_case_rows",
            return_value=[
                {
                    "caseNo": "JQ001",
                    "callTime": "2026-04-01 08:00:00",
                    "dutyDeptName": "一中队",
                    "cmdName": "云城",
                    "caseContents": "校园欺凌警情",
                    "replies": "已处置",
                    "occurAddress": "学校",
                },
                {"caseNo": "JQ002", "caseContent": "未转案"},
            ],
        ) as mock_fetch_incidents, patch(
            "hqzcsj.service.wcnr_10lv_service.get_database_connection",
            return_value=fake_conn,
        ), patch(
            "hqzcsj.service.wcnr_10lv_service.wcnr_case_list_dao.fetch_cases_by_incident_numbers",
            return_value={
                "JQ001": [
                    {
                        "ajxx_ajbh": "AJ001",
                        "ajxx_ajmc": "案件1",
                        "ajxx_lasj": "2026-04-02 00:00:00",
                    }
                ]
            },
        ) as mock_fetch_cases:
            rows = build_campus_bullying_incident_case_export_rows(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-30 23:59:59",
            )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["警情编号"], "JQ001")
        self.assertEqual(rows[0]["报警内容"], "校园欺凌警情")
        self.assertEqual(rows[0]["案件编号"], "AJ001")
        self.assertEqual(rows[1]["警情编号"], "JQ002")
        self.assertEqual(rows[1]["报警内容"], "未转案")
        self.assertEqual(rows[1]["案件编号"], "")
        self.assertEqual(mock_fetch_incidents.call_args.kwargs["start_time"], "2026-04-01 00:00:00")
        self.assertEqual(mock_fetch_cases.call_args.args[1], ["JQ001", "JQ002"])
        fake_conn.close.assert_called_once()

    def test_campus_bullying_export_title_uses_query_range(self) -> None:
        self.assertEqual(
            build_campus_bullying_export_title(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-30 23:59:59",
            ),
            "2026-04-01 00:00:00至2026-04-30 23:59:59校园欺凌警情案件",
        )


if __name__ == "__main__":
    unittest.main()
