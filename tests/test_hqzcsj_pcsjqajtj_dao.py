import unittest

from hqzcsj.dao.pcsjqajtj_dao import fetch_detail_rows, fetch_summary_rows


class _FakeCursor:
    def __init__(self, *, rows=None, description=None):
        self.rows = rows or []
        self.description = description or [("dummy",)]
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursors):
        self._cursors = list(cursors)
        self.created = []

    def cursor(self):
        if not self._cursors:
            raise AssertionError("No fake cursor prepared")
        cursor = self._cursors.pop(0)
        self.created.append(cursor)
        return cursor


class TestPcsjqajtjDao(unittest.TestCase):
    def test_summary_daibu_qisu_filters_use_ajxx_aymc(self) -> None:
        cursor = _FakeCursor(rows=[], description=[("所属分局",)])
        conn = _FakeConnection([cursor])

        rows = fetch_summary_rows(
            conn,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-08 00:00:00",
            leixing_list=["治安"],
            ssfjdm_list=[],
        )

        self.assertEqual(rows, [])
        sql, params = cursor.executed[-1]
        self.assertIn("LEFT JOIN ywdata.zq_zfba_ajxx aj_dbz", sql)
        self.assertIn("aj_dbz.ajxx_ajbh = dbz.ajxx_ajbh", sql)
        self.assertIn("LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000' AS pcsdm", sql)
        self.assertNotIn("LEFT(dbz.dbz_cbqy_bh_dm, 8) || '0000' AS pcsdm", sql)
        self.assertIn("COALESCE(aj_dbz.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern", sql)
        self.assertNotIn("COALESCE(dbz.dbz_ay_mc, '') SIMILAR TO ctc.ay_pattern", sql)
        self.assertIn("LEFT JOIN ywdata.zq_zfba_ajxx aj_qs", sql)
        self.assertIn("aj_qs.ajxx_ajbh = qsryxx.ajxx_ajbh", sql)
        self.assertIn("COALESCE(aj_qs.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern", sql)
        self.assertNotIn("COALESCE(qsryxx.ajxx_ay, '') SIMILAR TO ctc.ay_pattern", sql)
        self.assertEqual(params[2], ["治安"])

    def test_detail_daibu_filter_uses_ajxx_aymc(self) -> None:
        patterns_cursor = _FakeCursor(rows=[(".*",)])
        detail_cursor = _FakeCursor(rows=[], description=[("逮捕证ID",)])
        conn = _FakeConnection([patterns_cursor, detail_cursor])

        rows, truncated = fetch_detail_rows(
            conn,
            metric="逮捕",
            pcsdm="__ALL__",
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-08 00:00:00",
            leixing_list=["治安"],
            limit=0,
        )

        self.assertEqual(rows, [])
        self.assertFalse(truncated)
        sql, params = detail_cursor.executed[-1]
        self.assertIn("LEFT JOIN ywdata.zq_zfba_ajxx aj_dbz", sql)
        self.assertIn("aj_dbz.ajxx_ajbh = dbz.ajxx_ajbh", sql)
        self.assertIn("LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000' AS \"派出所代码\"", sql)
        self.assertNotIn("LEFT(dbz.dbz_cbqy_bh_dm, 8) || '0000' AS \"派出所代码\"", sql)
        self.assertIn("COALESCE(aj_dbz.ajxx_aymc, '') SIMILAR TO p.pattern", sql)
        self.assertNotIn("COALESCE(dbz.dbz_ay_mc, '') SIMILAR TO p.pattern", sql)
        self.assertEqual(params[2], [".*"])

    def test_detail_qisu_filter_uses_ajxx_aymc(self) -> None:
        patterns_cursor = _FakeCursor(rows=[(".*",)])
        detail_cursor = _FakeCursor(rows=[], description=[("起诉ID",)])
        conn = _FakeConnection([patterns_cursor, detail_cursor])

        rows, truncated = fetch_detail_rows(
            conn,
            metric="起诉",
            pcsdm="__ALL__",
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-08 00:00:00",
            leixing_list=["治安"],
            limit=0,
        )

        self.assertEqual(rows, [])
        self.assertFalse(truncated)
        sql, params = detail_cursor.executed[-1]
        self.assertIn("LEFT JOIN ywdata.zq_zfba_ajxx aj_qs", sql)
        self.assertIn("aj_qs.ajxx_ajbh = qs.ajxx_ajbh", sql)
        self.assertIn("COALESCE(aj_qs.ajxx_aymc, '') SIMILAR TO p.pattern", sql)
        self.assertNotIn("COALESCE(qs.ajxx_ay, '') SIMILAR TO p.pattern", sql)
        self.assertEqual(params[2], [".*"])


if __name__ == "__main__":
    unittest.main()
