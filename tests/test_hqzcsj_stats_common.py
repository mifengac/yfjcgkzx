import unittest
from datetime import datetime

from hqzcsj.service import stats_common


class TestStatsCommon(unittest.TestCase):
    def test_normalize_text_list_strips_and_drops_empty_values(self) -> None:
        self.assertEqual(
            stats_common.normalize_text_list([" 云城 ", "", None, "罗定", "   "]),
            ["云城", "罗定"],
        )

    def test_parse_dt_accepts_second_and_minute_precision(self) -> None:
        self.assertEqual(
            stats_common.parse_dt("2026-04-14 08:30:15"),
            datetime(2026, 4, 14, 8, 30, 15),
        )
        self.assertEqual(
            stats_common.parse_dt("2026-04-14 08:30"),
            datetime(2026, 4, 14, 8, 30, 0),
        )

    def test_shift_year_handles_leap_day(self) -> None:
        self.assertEqual(
            stats_common.shift_year(datetime(2024, 2, 29, 10, 0, 0), -1),
            datetime(2023, 2, 28, 10, 0, 0),
        )

    def test_calc_percent_text_and_ratio_text_keep_existing_format(self) -> None:
        self.assertEqual(stats_common.calc_percent_text(3, 4), "75.00%")
        self.assertEqual(stats_common.calc_percent_text(1, 0), "0.00%")
        self.assertEqual(stats_common.calc_ratio_text(10, 10, "起"), "持平")
        self.assertEqual(stats_common.calc_ratio_text(0, 5, "起"), "下降5起")
        self.assertEqual(stats_common.calc_ratio_text(5, 0, "起"), "上升5起")


if __name__ == "__main__":
    unittest.main()