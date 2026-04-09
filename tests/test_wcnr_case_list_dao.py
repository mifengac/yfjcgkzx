import unittest
from unittest.mock import patch

from hqzcsj.dao import wcnr_case_list_dao


class TestWcnrCaseListDao(unittest.TestCase):
    def test_build_payload_uses_required_filters_and_page_size(self) -> None:
        payload = wcnr_case_list_dao._build_minor_case_payload(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
            page_num=3,
        )

        self.assertEqual(payload["beginDate"], "2026-01-01 00:00:00")
        self.assertEqual(payload["endDate"], "2026-01-02 00:00:00")
        self.assertEqual(payload["newCharaSubclassNo"], "")
        self.assertEqual(payload["newCharaSubclass"], "全部")
        self.assertEqual(payload["caseMarkNo"], "01020201,0102020101,0102020102,0102020103")
        self.assertEqual(payload["caseMark"], "未成年人,未成年人（加害方）,未成年人（受害方）,未成年人（其他）")
        self.assertEqual(payload["pageSize"], "2000")
        self.assertEqual(payload["pageNum"], "3")
        self.assertEqual(payload["orderByColumn"], "callTime")
        self.assertEqual(payload["isAsc"], "desc")

    def test_fetch_minor_case_rows_paginates_until_total(self) -> None:
        responses = [
            {"code": 0, "total": 3, "rows": [{"caseNo": "A1"}, {"caseNo": "A2"}]},
            {"code": 0, "total": 3, "rows": [{"caseNo": "A3"}]},
        ]

        with patch.object(wcnr_case_list_dao, "CASE_LIST_PAGE_SIZE", 2), patch(
            "hqzcsj.dao.wcnr_case_list_dao.api_client.get_case_list",
            side_effect=responses,
        ) as mock_get_case_list:
            rows = wcnr_case_list_dao.fetch_minor_case_rows(
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
            )

        self.assertEqual([row["caseNo"] for row in rows], ["A1", "A2", "A3"])
        self.assertEqual(mock_get_case_list.call_count, 2)
        first_payload = mock_get_case_list.call_args_list[0].args[0]
        second_payload = mock_get_case_list.call_args_list[1].args[0]
        self.assertEqual(first_payload["pageSize"], "2")
        self.assertEqual(first_payload["pageNum"], "1")
        self.assertEqual(second_payload["pageNum"], "2")

    def test_fetch_minor_case_rows_raises_on_upstream_timeout(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_case_list_dao.api_client.get_case_list",
            return_value={"code": -1, "rows": [], "total": 0},
        ):
            with self.assertRaisesRegex(RuntimeError, "111警情系统登录或取数超时"):
                wcnr_case_list_dao.fetch_minor_case_rows(
                    start_time="2026-01-01 00:00:00",
                    end_time="2026-01-02 00:00:00",
                )

    def test_fetch_minor_case_rows_raises_on_nonzero_code_and_bad_rows(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_case_list_dao.api_client.get_case_list",
            return_value={"code": 7, "msg": "boom", "rows": [], "total": 0},
        ):
            with self.assertRaisesRegex(RuntimeError, "case/list 返回异常，code=7"):
                wcnr_case_list_dao.fetch_minor_case_rows(
                    start_time="2026-01-01 00:00:00",
                    end_time="2026-01-02 00:00:00",
                )

        with patch(
            "hqzcsj.dao.wcnr_case_list_dao.api_client.get_case_list",
            return_value={"code": 0, "msg": "", "rows": {}, "total": 0},
        ):
            with self.assertRaisesRegex(RuntimeError, "case/list rows 不是数组"):
                wcnr_case_list_dao.fetch_minor_case_rows(
                    start_time="2026-01-01 00:00:00",
                    end_time="2026-01-02 00:00:00",
                )

    def test_filter_rows_by_subclasses_supports_none_single_multi_and_empty(self) -> None:
        rows = [
            {"caseNo": "A1", "newCharaSubclass": "02020201"},
            {"caseNo": "A2", "newcharasubclass": "01020301"},
            {"caseNo": "A3", "newCharaSubclass": "12010000"},
        ]

        all_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(rows, subclass_codes=None)
        single_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(
            rows, subclass_codes=["02020201"]
        )
        multi_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(
            rows, subclass_codes=["02020201", "01020301"]
        )
        empty_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(rows, subclass_codes=[])

        self.assertEqual([row["caseNo"] for row in all_rows], ["A1", "A2"])
        self.assertEqual([row["caseNo"] for row in single_rows], ["A1"])
        self.assertEqual([row["caseNo"] for row in multi_rows], ["A1", "A2"])
        self.assertEqual(empty_rows, [])

    def test_detail_rows_use_region_fallback_sorting_and_limit(self) -> None:
        raw_rows = [
            {
                "caseNo": "A1",
                "callTime": "2026-01-02 10:00:00",
                "dutyDeptName": "单位1",
                "cmdName": "分局1",
                "occurAddress": "地址1",
                "caseContents": "内容1",
                "replies": "处警1",
                "caseMark": "未成年人",
                "lngOfCriterion": "111",
                "latOfCriterion": "22",
                "cmdid": "445302999999",
            },
            {
                "caseNo": "A2",
                "callTime": "2026-01-01 08:00:00",
                "dutydeptname": "单位2",
                "cmdname": "分局2",
                "occuraddress": "地址2",
                "casecontents": "内容2",
                "replies": "处警2",
                "casemark": "未成年人（受害方）",
                "lngofcriterion": "112",
                "latofcriterion": "23",
                "dutyDeptNo": "445303888888",
            },
        ]

        count_map = wcnr_case_list_dao.count_minor_case_rows_by_region(raw_rows)
        detail_rows, truncated = wcnr_case_list_dao.build_minor_case_detail_rows(
            raw_rows,
            diqu="__ALL__",
            limit=1,
        )
        region_rows, region_truncated = wcnr_case_list_dao.build_minor_case_detail_rows(
            raw_rows,
            diqu="445303",
            limit=0,
        )

        self.assertEqual(count_map, {"445302": 1, "445303": 1})
        self.assertTrue(truncated)
        self.assertEqual(detail_rows[0]["警情编号"], "A1")
        self.assertEqual(detail_rows[0]["地区"], "445302")
        self.assertFalse(region_truncated)
        self.assertEqual(len(region_rows), 1)
        self.assertEqual(region_rows[0]["警情编号"], "A2")
        self.assertEqual(region_rows[0]["地区"], "445303")


if __name__ == "__main__":
    unittest.main()
