import unittest

from xxffmk.dao import xxffmk_dao


class TestXxffmkDao(unittest.TestCase):
    def test_dimension5_query_uses_night_day_mv(self) -> None:
        sql = xxffmk_dao.build_dimension5_query()
        self.assertIn('"ywdata"."mv_xxffmk_dim5_night_day"', sql)
        self.assertNotIn('ywdata.str_to_ts(t."shot_time")', sql)
        self.assertIn('n.shot_date >= p.begin_date', sql)
        self.assertIn('n.shot_date <= p.end_date', sql)

    def test_dimension2_query_filters_case_subclass(self) -> None:
        sql = xxffmk_dao.build_dimension2_query()
        self.assertIn('j."newcharasubclass" IN (\'01\',\'02\',\'04\',\'05\',\'06\',\'08\',\'09\')', sql)

    def test_dimension1_detail_query_uses_raw_rows(self) -> None:
        sql = xxffmk_dao.build_dimension1_detail_query(["AB中学", "CD中学"])
        self.assertIn('FROM "ywdata"."zq_zfba_wcnr_sfzxx" z', sql)
        self.assertIn('BTRIM(COALESCE(z."yxx", \'\')) IN (%s, %s)', sql)
        self.assertNotIn('GROUP BY', sql)

    def test_refresh_statements_cover_materialized_views(self) -> None:
        statements = xxffmk_dao.build_refresh_materialized_views_statements()
        self.assertEqual(len(statements), 3)
        self.assertIn('REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_school_dim"', statements)
        self.assertIn('REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_student_school_rel"', statements)
        self.assertIn('REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_dim5_night_day"', statements)

    def test_student_school_rel_cte_keeps_vocational_priority(self) -> None:
        sql = xxffmk_dao.build_student_school_rel_cte()
        self.assertIn("'zzxj' AS source_type", sql)
        self.assertIn("1 AS source_priority", sql)
        self.assertIn("'zxxj' AS source_type", sql)
        self.assertIn("2 AS source_priority", sql)
        self.assertIn('PARTITION BY ss."sfzjh"', sql)

    def test_paginate_rows_returns_expected_slice(self) -> None:
        rows = [{"id": i} for i in range(1, 11)]
        payload = xxffmk_dao.paginate_rows(rows, page=2, page_size=3)
        self.assertEqual(payload["rows"], [{"id": 4}, {"id": 5}, {"id": 6}])
        self.assertEqual(payload["total"], 10)
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["page_size"], 3)


if __name__ == "__main__":
    unittest.main()
