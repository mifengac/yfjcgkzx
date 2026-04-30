import unittest
from unittest.mock import patch

from gzrzdd.dao import gzrzdd_cqtj_dao, gzrzdd_dao


class TestGzrzddDaoTimeSql(unittest.TestCase):
    def test_repeat_stats_sql_does_not_hardcode_start_date(self) -> None:
        sql = gzrzdd_dao.DEFAULT_GZRZ_SQL
        self.assertNotIn("2025-1-1", sql)
        self.assertIn("WHERE 1=1", sql)

    def test_repeat_stats_query_appends_selected_work_time_range(self) -> None:
        with patch.object(gzrzdd_dao, "query_to_dataframe") as query_mock:
            gzrzdd_dao.query_gzrz_by_work_time("2024-01-01 00:00:00", "2024-02-01 00:00:00")

        sql, params = query_mock.call_args.args
        self.assertIn("AND c.kzgzsj >= %s", sql)
        self.assertIn("AND c.kzgzsj <= %s", sql)
        self.assertEqual(params, ["2024-01-01 00:00:00", "2024-02-01 00:00:00"])

    def test_cqtj_sql_does_not_hardcode_start_date(self) -> None:
        sql = gzrzdd_cqtj_dao.DEFAULT_ZDRYGZRZS_SQL
        self.assertNotIn("2025-1-1", sql)
        self.assertIn("WHERE 1=1", sql)

    def test_cqtj_query_appends_selected_work_time_range(self) -> None:
        with patch.object(gzrzdd_cqtj_dao, "query_to_dataframe") as query_mock:
            gzrzdd_cqtj_dao.load_zdrygzrzs(
                start_time="2024-01-01 00:00:00",
                end_time="2024-02-01 00:00:00",
            )

        sql, params = query_mock.call_args.args
        self.assertIn("AND c.kzgzsj >= %s", sql)
        self.assertIn("AND c.kzgzsj <= %s", sql)
        self.assertEqual(params, ["2024-01-01 00:00:00", "2024-02-01 00:00:00"])


if __name__ == "__main__":
    unittest.main()
