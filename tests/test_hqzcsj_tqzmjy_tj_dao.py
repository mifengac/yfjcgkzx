import unittest

from hqzcsj.dao.tqzmjy_tj_dao import fetch_rows


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

    def cursor(self):
        if not self._cursors:
            raise AssertionError("No fake cursor prepared")
        return self._cursors.pop(0)


class TestTqzmjyTjDao(unittest.TestCase):
    def test_fetch_rows_uses_name_and_case_join_and_filters(self) -> None:
        cursor = _FakeCursor(
            rows=[("A001", "测试案件", "2026-04-01 08:00:00", "文书", "张三", "4401", "行政", "445300000000", "单位", "案由", "户籍", "现住")],
            description=[
                ("案件编号",),
                ("案件名称",),
                ("审批时间",),
                ("文书名称",),
                ("姓名",),
                ("身份证号",),
                ("案件类型",),
                ("地区",),
                ("承办单位",),
                ("案由",),
                ("户籍地址",),
                ("现住地",),
            ],
        )
        conn = _FakeConnection([cursor])

        rows = fetch_rows(
            conn,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-08 00:00:00",
            leixing_list=["治安"],
            ssfjdm_list=["445302000000"],
        )

        self.assertEqual(rows[0]["案件编号"], "A001")
        sql, params = cursor.executed[-1]
        self.assertIn("FROM ywdata.zq_zfba_tqzmjy t", sql)
        self.assertIn("LEFT JOIN ywdata.zq_zfba_xyrxx x", sql)
        self.assertIn("t.ajbh = x.ajxx_join_ajxx_ajbh", sql)
        self.assertIn("COALESCE(BTRIM(t.xgry_xm), '') = COALESCE(BTRIM(x.xyrxx_xm), '')", sql)
        self.assertIn("(LEFT(COALESCE(x.ajxx_join_ajxx_cbdw_bh_dm, ''), 6) || '000000') = ANY(p.ssfjdm_list)", sql)
        self.assertIn("COALESCE(x.ajxx_join_ajxx_ay, '') SIMILAR TO ctc.ay_pattern", sql)
        self.assertIn("LEFT(x.ajxx_join_ajxx_cbdw_bh_dm, 6) || '000000'", sql)
        self.assertEqual(params[2], ["治安"])
        self.assertEqual(params[3], ["445302000000"])


if __name__ == "__main__":
    unittest.main()
