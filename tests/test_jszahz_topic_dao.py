import unittest
from unittest.mock import patch

from jszahzyj.dao import jszahz_topic_dao


class TestJszahzTopicDao(unittest.TestCase):
    def test_query_live_person_rows_uses_parameterized_managed_filter(self) -> None:
        with patch.object(jszahz_topic_dao, "execute_query", return_value=[]) as mock_execute:
            jszahz_topic_dao.query_live_person_rows(managed_only=True)

        sql, params = mock_execute.call_args[0]
        self.assertIn("WITH live_people AS", sql)
        self.assertIn("SELECT DISTINCT ON (lp.zjhm)", sql)
        self.assertIn("ORDER BY lp.zjhm", sql)
        self.assertIn("AND (NOT %s OR p.sflg = '1')", sql)
        self.assertEqual(params, (True,))

    def test_query_live_person_rows_passes_false_flag_through(self) -> None:
        with patch.object(jszahz_topic_dao, "execute_query", return_value=[]) as mock_execute:
            jszahz_topic_dao.query_live_person_rows(managed_only=False)

        sql, params = mock_execute.call_args[0]
        self.assertIn("AND (NOT %s OR p.sflg = '1')", sql)
        self.assertEqual(params, (False,))


if __name__ == "__main__":
    unittest.main()
