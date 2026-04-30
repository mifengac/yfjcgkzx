import unittest
from unittest.mock import patch

import pandas as pd

from gzrzdd.service import gzrzdd_service as service


def _sample_logs() -> pd.DataFrame:
    base = {
        "姓名": "张三",
        "证件号码": "ID-A",
        "分局名称": "云城",
        "所属派出所": "城南派出所",
        "列管时间": "2026-01-01 00:00:00",
        "工作日志工作类型": "走访",
        "工作日志工作情况说明": "持续走访化解矛盾纠纷，情况稳定",
        "工作日志系统登记时间": "2026-01-01 12:00:00",
    }
    rows = []
    for work_time in (
        "2026-01-01 10:00:00",
        "2026-01-05 10:00:00",
        "2026-02-01 10:00:00",
    ):
        item = dict(base)
        item["工作日志开展工作时间"] = work_time
        rows.append(item)
    return pd.DataFrame(rows)


class TestGzrzddService(unittest.TestCase):
    def setUp(self) -> None:
        service.CACHE.clear()

    def tearDown(self) -> None:
        service.CACHE.clear()

    def test_compute_stats_filters_by_work_log_time_before_duplicate_stats(self) -> None:
        with patch.object(service, "query_gzrz_by_work_time", return_value=_sample_logs()) as query_mock:
            result_id, pivot = service.compute_stats(
                count=5,
                threshold_percent=80,
                start_time="2026-01-01T00:00",
                end_time="2026-01-31T23:59:59",
            )

        start_arg, end_arg = query_mock.call_args.args
        self.assertEqual(start_arg.strftime("%Y-%m-%d %H:%M:%S"), "2026-01-01 00:00:00")
        self.assertEqual(end_arg.strftime("%Y-%m-%d %H:%M:%S"), "2026-01-31 23:59:59")
        self.assertEqual(pivot["rows"], ["合计"])
        self.assertEqual(pivot["cols"], ["云城"])
        self.assertEqual(pivot["data"], [[1]])

        records = service.get_detail_records(result_id, branch="云城", station="")
        self.assertEqual(len(records), 1)
        work_times = records[0]["工作日志开展工作时间"]
        self.assertIn("2026-01-01", work_times)
        self.assertIn("2026-01-05", work_times)
        self.assertNotIn("2026-02-01", work_times)

    def test_compute_stats_rejects_invalid_work_log_time_range_before_querying(self) -> None:
        with patch.object(service, "query_gzrz_by_work_time") as query_mock:
            with self.assertRaisesRegex(ValueError, "工作日志开始时间不能大于结束时间"):
                service.compute_stats(
                    count=5,
                    threshold_percent=80,
                    start_time="2026-02-01 00:00:00",
                    end_time="2026-01-01 00:00:00",
                )
        query_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
