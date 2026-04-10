import sys
import types
import unittest


class Workbook:
    pass


fake_pandas = types.ModuleType("pandas")
fake_pandas.Timestamp = type("Timestamp", (), {})
fake_pandas.isna = lambda value: value is None
fake_pandas.to_datetime = lambda value, errors=None: value
sys.modules.setdefault("pandas", fake_pandas)

fake_openpyxl = types.ModuleType("openpyxl")
fake_openpyxl.Workbook = Workbook
sys.modules.setdefault("openpyxl", fake_openpyxl)

fake_cqtj_dao = types.ModuleType("gzrzdd.dao.gzrzdd_cqtj_dao")
fake_cqtj_dao.load_zdrygzrzs = lambda: None
sys.modules.setdefault("gzrzdd.dao.gzrzdd_cqtj_dao", fake_cqtj_dao)

fake_dao = types.ModuleType("gzrzdd.dao.gzrzdd_dao")
fake_dao.find_col = lambda df, col: col
sys.modules.setdefault("gzrzdd.dao.gzrzdd_dao", fake_dao)

from gzrzdd.service import gzrzdd_cqtj_service as service


class TestGzrzddCqtjService(unittest.TestCase):
    def test_status_by_risk_uses_updated_thresholds(self) -> None:
        cases = [
            ("高风险", 6, ("ok", "normal")),
            ("高风险", 7, ("remind", "yellow")),
            ("高风险", 8, ("warn", "red")),
            ("中风险", 13, ("ok", "normal")),
            ("中风险", 14, ("remind", "yellow")),
            ("中风险", 16, ("warn", "red")),
            ("低风险", 20, ("ok", "normal")),
            ("低风险", 21, ("remind", "yellow")),
            ("低风险", 31, ("warn", "red")),
        ]

        for risk, days, expected in cases:
            with self.subTest(risk=risk, days=days):
                self.assertEqual(service._status_by_risk(risk, days), expected)


if __name__ == "__main__":
    unittest.main()
