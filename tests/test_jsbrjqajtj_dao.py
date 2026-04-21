import unittest
from unittest.mock import patch

from jszahzyj.dao import jsbrjqajtj_dao


class TestJsbrjqajtjDao(unittest.TestCase):
    def test_normalize_datetime_text_supports_html_datetime_value(self) -> None:
        result = jsbrjqajtj_dao.normalize_datetime_text("2026-04-01T08:30")

        self.assertEqual(result, "2026-04-01 08:30:00")

    def test_query_jsbrjqajtj_adds_branch_filter_when_branch_selected(self) -> None:
        with patch.object(jsbrjqajtj_dao, "execute_query", return_value=[]) as mock_execute:
            jsbrjqajtj_dao.query_jsbrjqajtj(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                branches=["445302000000", "445303000000"],
            )

        sql, params = mock_execute.call_args[0]
        self.assertIn("cmdid = ANY(%s)", sql)
        self.assertIn("spiritcausetrouble IS NOT NULL", sql)
        self.assertIn("newcharacategory AS 警情性质", sql)
        self.assertIn("jq.警情性质", sql)
        self.assertIn("LEFT JOIN ywdata.zq_zfba_ajxx aj", sql)
        self.assertEqual(
            params,
            (
                "2026-04-01 00:00:00",
                "2026-04-08 00:00:00",
                ["445302000000", "445303000000"],
            ),
        )

    def test_query_jsbrjqajtj_omits_branch_filter_when_branch_empty(self) -> None:
        with patch.object(jsbrjqajtj_dao, "execute_query", return_value=[]) as mock_execute:
            jsbrjqajtj_dao.query_jsbrjqajtj(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                branches=["", "   "],
            )

        sql, params = mock_execute.call_args[0]
        self.assertNotIn("cmdid = ANY(%s)", sql)
        self.assertEqual(params, ("2026-04-01 00:00:00", "2026-04-08 00:00:00"))

    def test_query_jsbrjqajtj_rejects_inverted_time_range(self) -> None:
        with self.assertRaisesRegex(ValueError, "开始时间不能大于结束时间"):
            jsbrjqajtj_dao.query_jsbrjqajtj(
                start_time="2026-04-08 00:00:00",
                end_time="2026-04-01 00:00:00",
            )


if __name__ == "__main__":
    unittest.main()
