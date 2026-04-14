import unittest
from unittest.mock import patch

from jszahzyj.dao import jszahzyj_dao


class TestJszahzyjDao(unittest.TestCase):
    def test_build_query_sql_appends_filters_in_expected_order(self) -> None:
        sql, params = jszahzyj_dao._build_query_sql(
            liguan_start="2026-04-01 00:00:00",
            liguan_end="2026-04-08 00:00:00",
            maodun_start="2026-04-02 00:00:00",
            maodun_end="2026-04-09 00:00:00",
            fenju_list=["云城分局", "云安分局"],
        )

        self.assertIn("a.lgsj >= %s", sql)
        self.assertIn("a.lgsj <= %s", sql)
        self.assertIn("b.mdjfdjsj >= %s", sql)
        self.assertIn("b.mdjfdjsj <= %s", sql)
        self.assertIn(") = ANY(%s)", sql)
        self.assertIn("ORDER BY a.lgsj DESC", sql)
        self.assertEqual(
            params,
            [
                "2026-04-01 00:00:00",
                "2026-04-08 00:00:00",
                "2026-04-02 00:00:00",
                "2026-04-09 00:00:00",
                ["云城分局", "云安分局"],
            ],
        )

    def test_query_jszahzyj_data_wraps_total_count_and_pagination(self) -> None:
        mocked_rows = [
            {"total_count": 15, "姓名": "张三", "证件号码": "440123199001011111"},
            {"total_count": 15, "姓名": "李四", "证件号码": "440123199001011112"},
        ]

        with patch.object(jszahzyj_dao, "execute_query", return_value=mocked_rows) as mock_execute:
            rows, total = jszahzyj_dao.query_jszahzyj_data(
                liguan_start="2026-04-01 00:00:00",
                liguan_end="2026-04-08 00:00:00",
                fenju_list=["云城分局"],
                page=2,
                page_size=10,
            )

        sql, params = mock_execute.call_args[0]
        self.assertIn("COUNT(*) OVER() AS total_count", sql)
        self.assertIn("LIMIT %s OFFSET %s", sql)
        self.assertIn("ORDER BY a.lgsj DESC", sql)
        self.assertIn("= ANY(%s)", sql)
        self.assertEqual(
            params,
            (
                "2026-04-01 00:00:00",
                "2026-04-08 00:00:00",
                ["云城分局"],
                10,
                10,
            ),
        )
        self.assertEqual(total, 15)
        self.assertEqual(rows, [{"姓名": "张三", "证件号码": "440123199001011111"}, {"姓名": "李四", "证件号码": "440123199001011112"}])

    def test_get_all_jszahzyj_data_does_not_append_pagination(self) -> None:
        with patch.object(jszahzyj_dao, "execute_query", return_value=[]) as mock_execute:
            jszahzyj_dao.get_all_jszahzyj_data(
                maodun_start="2026-04-02 00:00:00",
                maodun_end="2026-04-09 00:00:00",
            )

        sql, params = mock_execute.call_args[0]
        self.assertNotIn("COUNT(*) OVER()", sql)
        self.assertNotIn("LIMIT %s OFFSET %s", sql)
        self.assertIn("b.mdjfdjsj >= %s", sql)
        self.assertIn("b.mdjfdjsj <= %s", sql)
        self.assertEqual(params, ("2026-04-02 00:00:00", "2026-04-09 00:00:00"))


if __name__ == "__main__":
    unittest.main()