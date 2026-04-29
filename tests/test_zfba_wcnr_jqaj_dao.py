import unittest
from unittest.mock import patch

from hqzcsj.dao import zfba_wcnr_jqaj_dao


class _SqlCaptureCursor:
    def __init__(self):
        self.description = [("警情编号",)]
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((str(query), params))

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _SqlCaptureConnection:
    def __init__(self):
        self.cursor_obj = _SqlCaptureCursor()

    def cursor(self):
        return self.cursor_obj


class TestZfbaWcnrJqajDao(unittest.TestCase):
    def test_count_jq_by_diqu_defaults_to_01_02_filter_when_type_not_selected(self) -> None:
        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_minor_case_rows",
            return_value=[{"caseNo": "A1"}],
        ) as mock_fetch_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.filter_minor_case_rows_by_subclasses",
            return_value=[{"caseNo": "A1"}],
        ) as mock_filter_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.count_minor_case_rows_by_region",
            return_value={"445302": 1},
        ) as mock_count_rows:
            result = zfba_wcnr_jqaj_dao.count_jq_by_diqu(
                object(),
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
            )

        self.assertEqual(result, {"445302": 1})
        mock_fetch_rows.assert_called_once_with(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
        )
        mock_filter_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            subclass_codes=None,
        )
        mock_count_rows.assert_called_once_with([{"caseNo": "A1"}])

    def test_count_jq_by_diqu_uses_case_list_helpers(self) -> None:
        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_newcharasubclass_list",
            return_value=["02020201"],
        ) as mock_fetch_subclasses, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_minor_case_rows",
            return_value=[{"caseNo": "A1"}],
        ) as mock_fetch_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.filter_minor_case_rows_by_subclasses",
            return_value=[{"caseNo": "A1"}],
        ) as mock_filter_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.count_minor_case_rows_by_region",
            return_value={"445302": 1},
        ) as mock_count_rows:
            result = zfba_wcnr_jqaj_dao.count_jq_by_diqu(
                object(),
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual(result, {"445302": 1})
        self.assertTrue(mock_fetch_subclasses.called)
        mock_fetch_rows.assert_called_once_with(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
        )
        mock_filter_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            subclass_codes=["02020201"],
        )
        mock_count_rows.assert_called_once_with([{"caseNo": "A1"}])

    def test_fetch_detail_rows_for_jq_uses_case_list_helpers(self) -> None:
        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_ay_patterns",
            return_value=[],
        ), patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_newcharasubclass_list",
            return_value=["02020201"],
        ), patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_minor_case_rows",
            return_value=[{"caseNo": "A1"}],
        ) as mock_fetch_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.filter_minor_case_rows_by_subclasses",
            return_value=[{"caseNo": "A1"}],
        ) as mock_filter_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.build_minor_case_detail_rows",
            return_value=([{"警情编号": "A1"}], True),
        ) as mock_build_rows:
            rows, truncated = zfba_wcnr_jqaj_dao.fetch_detail_rows(
                object(),
                metric="警情",
                diqu="445302",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
                za_types=[],
                limit=100,
            )

        self.assertEqual(rows, [{"警情编号": "A1"}])
        self.assertTrue(truncated)
        mock_fetch_rows.assert_called_once_with(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
        )
        mock_filter_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            subclass_codes=["02020201"],
        )
        mock_build_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            diqu="445302",
            limit=100,
        )

    def test_fetch_detail_rows_for_jq_defaults_to_01_02_filter_when_type_not_selected(self) -> None:
        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_ay_patterns",
            return_value=[],
        ), patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_minor_case_rows",
            return_value=[{"caseNo": "A1"}],
        ) as mock_fetch_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.filter_minor_case_rows_by_subclasses",
            return_value=[{"caseNo": "A1"}],
        ) as mock_filter_rows, patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.build_minor_case_detail_rows",
            return_value=([{"警情编号": "A1"}], False),
        ) as mock_build_rows:
            rows, truncated = zfba_wcnr_jqaj_dao.fetch_detail_rows(
                object(),
                metric="警情",
                diqu="__ALL__",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
                za_types=[],
                limit=0,
            )

        self.assertEqual(rows, [{"警情编号": "A1"}])
        self.assertFalse(truncated)
        mock_fetch_rows.assert_called_once_with(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
        )
        mock_filter_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            subclass_codes=None,
        )
        mock_build_rows.assert_called_once_with(
            [{"caseNo": "A1"}],
            diqu="__ALL__",
            limit=0,
        )

    def test_fetch_detail_rows_for_jq_returns_empty_when_type_mapping_missing(self) -> None:
        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_ay_patterns",
            return_value=[],
        ), patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_newcharasubclass_list",
            return_value=[],
        ), patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_minor_case_rows",
        ) as mock_fetch_rows:
            rows, truncated = zfba_wcnr_jqaj_dao.fetch_detail_rows(
                object(),
                metric="警情",
                diqu="__ALL__",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
                za_types=[],
                limit=0,
            )

        self.assertEqual(rows, [])
        self.assertFalse(truncated)
        mock_fetch_rows.assert_not_called()

    def test_fetch_detail_rows_for_zhuanan_includes_case_name_and_cause_name(self) -> None:
        conn = _SqlCaptureConnection()

        with patch(
            "hqzcsj.dao.zfba_wcnr_jqaj_dao.fetch_ay_patterns",
            return_value=[],
        ):
            rows, truncated = zfba_wcnr_jqaj_dao.fetch_detail_rows(
                conn,
                metric="转案数",
                diqu="__ALL__",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
                za_types=[],
                limit=0,
            )

        sql_text = conn.cursor_obj.executed[-1][0]
        self.assertEqual(rows, [])
        self.assertFalse(truncated)
        self.assertIn('zza."ajxx_ajmc" AS "案件名称"', sql_text)
        self.assertIn('zza."ajxx_aymc" AS "案由名称"', sql_text)


if __name__ == "__main__":
    unittest.main()
