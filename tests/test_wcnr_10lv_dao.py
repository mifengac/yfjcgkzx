import unittest
from unittest.mock import patch

from hqzcsj.dao.wcnr_10lv_dao import (
    _is_zmjz_ratio_den_row,
    _is_zmjz_ratio_num_row,
    fetch_metric_detail_rows,
)


QUALIFIED = "\u662f\u5426\u7b26\u5408\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2"
APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2\u7533\u8bf7\u4e66"
LEGACY_APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8\u6559\u80b2\u7533\u8bf7\u4e66"


class TestWcnr10lvDao(unittest.TestCase):
    def test_zmjz_ratio_den_requires_qualified_flag(self) -> None:
        self.assertTrue(
            _is_zmjz_ratio_den_row(
                {
                    QUALIFIED: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_ratio_den_row(
                {
                    QUALIFIED: "\u5426",
                }
            )
        )

    def test_zmjz_ratio_num_requires_qualified_and_apply_flags(self) -> None:
        self.assertFalse(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u5426",
                    APPLY: "\u662f",
                }
            )
        )

        self.assertTrue(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    APPLY: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    APPLY: "\u5426",
                }
            )
        )

    def test_zmjz_ratio_num_accepts_legacy_apply_field_name(self) -> None:
        self.assertTrue(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    LEGACY_APPLY: "\u662f",
                }
            )
        )

    def test_jq_detail_still_uses_shared_jingqing_detail_loader(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["打架斗殴"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=([{"地区": "445302", "报警时间": "2026-01-01 00:00:00"}], False),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="jq",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual(rows[0]["地区"], "云城")
        self.assertEqual(rows[0]["地区代码"], "445302")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "警情")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_za_rate_denominator_uses_shared_jingqing_detail_loader(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["打架斗殴"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=([{"地区": "445302", "报警时间": "2026-01-01 00:00:00"}], False),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="za_rate",
                part="denominator",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual(rows[0]["地区"], "云城")
        self.assertEqual(rows[0]["地区代码"], "445302")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "警情")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_jq_changsuo_detail_filters_replies_keywords(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["打架斗殴"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=(
                [
                    {"地区": "445302", "报警时间": "2026-01-01 00:00:00", "处警情况": "夜总会聚集"},
                    {"地区": "445303", "报警时间": "2026-01-02 00:00:00", "处警情况": "普通处警"},
                ],
                False,
            ),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="jq_changsuo",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["地区"], "云城")
        self.assertEqual(rows[0]["地区代码"], "445302")
        self.assertEqual(rows[0]["处警情况"], "夜总会聚集")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "警情")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_aj_changsuo_detail_filters_ajxx_jyaq_keywords(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["打架斗殴"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_jq_aj_dao.fetch_ay_patterns",
            return_value=[".*"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_wcnr_ajxx_changsuo_base_rows",
            return_value=[
                {"地区": "445303", "案件编号": "A1", "简要案情": "KTV内发生争执"},
                {"地区": "445302", "案件编号": "A2", "简要案情": "普通治安案件"},
            ],
        ) as mock_fetch_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="aj_changsuo",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["地区"], "云安")
        self.assertEqual(rows[0]["地区代码"], "445303")
        self.assertEqual(rows[0]["简要案情"], "KTV内发生争执")
        self.assertEqual(mock_fetch_rows.call_args.kwargs["patterns"], [".*"])
        self.assertIsNone(mock_fetch_rows.call_args.kwargs["diqu"])


if __name__ == "__main__":
    unittest.main()
