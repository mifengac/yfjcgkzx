import unittest
from unittest.mock import patch

import pandas as pd

from gzrzdd.service import gzrzdd_cqtj_service as service


def _sample_cqtj_logs() -> pd.DataFrame:
    base = {
        "姓名": "李四",
        "证件号码": "ID-B",
        "风险等级": "高风险",
        "分局名称": "云城分局",
        "所属派出所": "城北派出所",
        "列管时间": "2026-01-01 00:00:00",
        "工作日志工作类型": "走访",
        "工作日志工作情况说明": "工作日志内容",
        "工作日志系统登记时间": "2026-01-01 12:00:00",
    }
    rows = []
    for work_time in (
        "2026-01-05 10:00:00",
    ):
        item = dict(base)
        item["工作日志开展工作时间"] = work_time
        rows.append(item)
    return pd.DataFrame(rows)


def _sample_cqtj_missing_work_time() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "姓名": "王五",
                "证件号码": "ID-C",
                "风险等级": "高风险",
                "分局名称": "云城分局",
                "所属派出所": "城西派出所",
                "列管时间": "2026-01-01 00:00:00",
                "工作日志开展工作时间": None,
                "工作日志工作类型": "",
                "工作日志工作情况说明": "",
                "工作日志系统登记时间": "",
            }
        ]
    )


class TestGzrzddCqtjTimeService(unittest.TestCase):
    def test_query_cqtj_passes_work_log_time_range_to_query(self) -> None:
        with patch.object(service, "load_zdrygzrzs", return_value=_sample_cqtj_logs()) as load_mock:
            _, records = service.query_cqtj(
                mode="detail",
                level="warn",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-31 23:59:59",
            )

        start_arg = load_mock.call_args.kwargs["start_time"]
        end_arg = load_mock.call_args.kwargs["end_time"]
        self.assertEqual(start_arg.strftime("%Y-%m-%d %H:%M:%S"), "2026-01-01 00:00:00")
        self.assertEqual(end_arg.strftime("%Y-%m-%d %H:%M:%S"), "2026-01-31 23:59:59")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["最近工作日志时间"], "2026-01-05 10:00:00")

    def test_query_cqtj_rejects_invalid_work_log_time_range_before_querying(self) -> None:
        with patch.object(service, "load_zdrygzrzs") as load_mock:
            with self.assertRaisesRegex(ValueError, "工作日志开始时间不能大于结束时间"):
                service.query_cqtj(
                    mode="detail",
                    level="warn",
                    start_time="2026-02-01 00:00:00",
                    end_time="2026-01-01 00:00:00",
                )
        load_mock.assert_not_called()

    def test_query_cqtj_keeps_missing_work_time_as_warn_without_nan_int_error(self) -> None:
        with patch.object(service, "load_zdrygzrzs", return_value=_sample_cqtj_missing_work_time()):
            _, records = service.query_cqtj(mode="detail", level="warn")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["状态"], "警告")
        self.assertEqual(records[0]["间隔天数"], "")


if __name__ == "__main__":
    unittest.main()
