import unittest
from unittest.mock import MagicMock, patch

from hqzcsj.service.wcnr_10lv_service import (
    build_detail_export_sheets,
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


if __name__ == "__main__":
    unittest.main()
