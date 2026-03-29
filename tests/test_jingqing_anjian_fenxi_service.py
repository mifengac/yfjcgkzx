import unittest
from datetime import datetime as real_datetime
from unittest.mock import patch

from jingqing_anjian_fenxi.service import jingqing_anjian_fenxi_service as service


class _FrozenDateTime(real_datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2026, 3, 29, 9, 30, 0)


class _DummyConnection:
    def close(self) -> None:
        return None


class TestJingqingAnjianFenxiService(unittest.TestCase):
    def test_default_time_range_for_page_uses_recent_7_days_midnight(self) -> None:
        with patch.object(service, "datetime", _FrozenDateTime):
            start_time, end_time = service.default_time_range_for_page()

        self.assertEqual(start_time, "2026-03-22 00:00:00")
        self.assertEqual(end_time, "2026-03-29 00:00:00")

    def test_build_summary_appends_weighted_total_row(self) -> None:
        group_rows = [
            {
                "fenju_name": "A分局",
                "fenju_code": "440100000000",
                "group_name": "甲县",
                "group_code": "440101000000",
            },
            {
                "fenju_name": "B分局",
                "fenju_code": "440200000000",
                "group_name": "乙县",
                "group_code": "440201000000",
            },
        ]
        filing_map = {
            "440101000000": {"sum_hours": 4, "row_count": 1},
            "440201000000": {"sum_hours": 20, "row_count": 4},
        }
        arrest_map = {
            "440101000000": {"sum_hours": 6, "row_count": 2},
        }
        solve_map = {
            "440201000000": {"sum_hours": 15, "row_count": 3},
        }
        close_map = {}

        with patch.object(service, "get_database_connection", return_value=_DummyConnection()), \
             patch.object(service.jingqing_anjian_fenxi_dao, "fetch_group_rows", return_value=group_rows), \
             patch.object(service.jingqing_anjian_fenxi_dao, "fetch_timely_filing_summary", return_value=filing_map), \
             patch.object(service.jingqing_anjian_fenxi_dao, "fetch_timely_arrest_summary", return_value=arrest_map), \
             patch.object(service.jingqing_anjian_fenxi_dao, "fetch_timely_solve_summary", return_value=solve_map), \
             patch.object(service.jingqing_anjian_fenxi_dao, "fetch_timely_close_summary", return_value=close_map):
            meta, rows = service.build_summary(
                start_time="2026-03-22 00:00:00",
                end_time="2026-03-29 00:00:00",
                group_mode="county",
                leixing_list=[],
                ssfjdm_list=[],
            )

        self.assertEqual(meta.group_mode, "county")
        self.assertEqual(meta.group_mode_label, "县市区")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["及时立案平均小时"], "4")
        self.assertEqual(rows[1]["及时立案平均小时"], "5")
        self.assertEqual(rows[2]["分局"], "全市")
        self.assertEqual(rows[2]["当前分组名称"], "全市")
        self.assertEqual(rows[2]["及时立案平均小时"], "4.8")
        self.assertEqual(rows[2]["及时研判抓人平均小时"], "3")
        self.assertEqual(rows[2]["及时破案平均小时"], "5")
        self.assertEqual(rows[2]["及时结案平均小时"], "")

    def test_fetch_detail_normalizes_group_mode_and_formats_time(self) -> None:
        with patch.object(service, "get_database_connection", return_value=_DummyConnection()), \
             patch.object(
                 service.jingqing_anjian_fenxi_dao,
                 "fetch_detail_rows",
                 return_value=([{"分局": "全市"}], False),
             ) as mock_fetch:
            rows, truncated = service.fetch_detail(
                metric="timely_filing",
                group_code="__ALL__",
                group_mode="station",
                start_time="2026-03-22 00:00:00",
                end_time="2026-03-29 00:00:00",
                leixing_list=["盗窃"],
                ssfjdm_list=["440100000000"],
                limit=5000,
            )

        self.assertFalse(truncated)
        self.assertEqual(rows, [{"分局": "全市"}])
        self.assertEqual(mock_fetch.call_args.kwargs["group_mode"], "station")
        self.assertEqual(mock_fetch.call_args.kwargs["start_time"], "2026-03-22 00:00:00")
        self.assertEqual(mock_fetch.call_args.kwargs["end_time"], "2026-03-29 00:00:00")
        self.assertEqual(mock_fetch.call_args.kwargs["leixing_list"], ["盗窃"])
        self.assertEqual(mock_fetch.call_args.kwargs["ssfjdm_list"], ["440100000000"])


if __name__ == "__main__":
    unittest.main()
