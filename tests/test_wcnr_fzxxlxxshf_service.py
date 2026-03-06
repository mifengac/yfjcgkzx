import unittest
from unittest.mock import Mock, patch

from weichengnianren.service.wcnr_fzxxlxxshf_service import (
    BRANCH_OPTIONS,
    defaults_payload,
    export_fzxxlxxshf_records,
    query_fzxxlxxshf_records,
)


class TestWcnrFzxxlxxshfService(unittest.TestCase):
    def test_defaults_payload_contains_expected_shape(self) -> None:
        payload = defaults_payload()

        self.assertTrue(payload["success"])
        self.assertEqual(payload["branches"], [])
        self.assertEqual(payload["page"], 1)
        self.assertEqual(payload["page_size"], 20)
        self.assertEqual(payload["branch_options"], BRANCH_OPTIONS)
        self.assertIn("start_time", payload)
        self.assertIn("end_time", payload)

    def test_query_rejects_empty_time(self) -> None:
        with self.assertRaises(ValueError):
            query_fzxxlxxshf_records(start_time="", end_time="2026-03-06 12:00:00")

    @patch("weichengnianren.service.wcnr_fzxxlxxshf_service.query_fzxxlxxshf_page")
    @patch("weichengnianren.service.wcnr_fzxxlxxshf_service.get_database_connection")
    def test_query_normalizes_pagination_and_branches(self, mock_get_conn: Mock, mock_query_page: Mock) -> None:
        conn = Mock()
        mock_get_conn.return_value = conn
        mock_query_page.return_value = ([{"姓名": "张三"}], 1)

        payload = query_fzxxlxxshf_records(
            start_time="2026-03-01 00:00",
            end_time="2026-03-06 23:59",
            branches=["云城分局", "非法分局"],
            page="0",
            page_size="999",
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["page"], 1)
        self.assertEqual(payload["page_size"], 20)
        self.assertEqual(payload["filters"]["branches"], ["云城分局"])
        mock_query_page.assert_called_once_with(
            conn,
            start_time="2026-03-01 00:00:00",
            end_time="2026-03-06 23:59:00",
            branches=["云城分局"],
            page=1,
            page_size=20,
        )
        conn.close.assert_called_once()

    @patch("weichengnianren.service.wcnr_fzxxlxxshf_service.query_fzxxlxxshf_all")
    @patch("weichengnianren.service.wcnr_fzxxlxxshf_service.get_database_connection")
    def test_export_uses_same_filters_and_returns_filename(self, mock_get_conn: Mock, mock_query_all: Mock) -> None:
        conn = Mock()
        mock_get_conn.return_value = conn
        mock_query_all.return_value = [{"姓名": "李四", "分局名称": "云安分局"}]

        data, mimetype, filename = export_fzxxlxxshf_records(
            fmt="csv",
            start_time="2026-03-01 08:00:00",
            end_time="2026-03-06 18:00:00",
            branches=["云安分局"],
        )

        self.assertTrue(data.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(mimetype, "text/csv; charset=utf-8")
        self.assertTrue(filename.endswith(".csv"))
        mock_query_all.assert_called_once_with(
            conn,
            start_time="2026-03-01 08:00:00",
            end_time="2026-03-06 18:00:00",
            branches=["云安分局"],
        )
        conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
