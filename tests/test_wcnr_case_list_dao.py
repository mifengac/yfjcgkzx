import unittest
from unittest.mock import patch

from hqzcsj.dao import wcnr_case_list_dao


class _FakeCursor:
    def __init__(self, rows=None):
        self.description = [
            ("ajxx_jqbh",),
            ("ajxx_ajbh",),
            ("ajxx_ajmc",),
            ("ajxx_lasj",),
        ]
        self.rows = rows or []
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
    def __init__(self, rows=None):
        self.cursor_obj = _FakeCursor(rows=rows)

    def cursor(self):
        return self.cursor_obj


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

    def test_build_province_minor_payload_uses_requested_filters(self) -> None:
        payload = wcnr_case_list_dao._build_province_minor_case_payload(
            start_time="2026-01-01 00:00:00",
            end_time="2026-01-02 00:00:00",
            page_num=3,
        )

        self.assertEqual(payload["params[startTime]"], "2026-01-01 00:00:00")
        self.assertEqual(payload["params[endTime]"], "2026-01-02 00:00:00")
        self.assertEqual(payload["charaNo"], "01,02")
        self.assertEqual(payload["chara"], "刑事类警情,行政（治安）类警情")
        self.assertEqual(payload["caseMarkNo"], "01020201,0102020101,0102020102,0102020103")
        self.assertEqual(payload["caseMark"], "未成年人,未成年人（加害方）,未成年人（受害方）,未成年人（其他）")
        self.assertIn("110报警", payload["caseSourceName"])
        self.assertEqual(payload["dutyDeptName"], "全部")
        self.assertEqual(payload["oriChara"], "全部")
        self.assertEqual(payload["fixCaseSourceName"], "全部")
        self.assertEqual(payload["pageSize"], "2000")
        self.assertEqual(payload["pageNum"], "3")
        self.assertEqual(payload["orderByColumn"], "alarmTime")
        self.assertEqual(payload["isAsc"], "desc")

    def test_build_campus_bullying_payload_uses_required_filters(self) -> None:
        payload = wcnr_case_list_dao._build_campus_bullying_case_payload(
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-30 23:59:59",
            page_num=2,
        )

        self.assertEqual(payload["beginDate"], "2026-04-01 00:00:00")
        self.assertEqual(payload["endDate"], "2026-04-30 23:59:59")
        self.assertEqual(payload["caseMarkNo"], "03010108,0604")
        self.assertEqual(payload["caseMark"], "校园欺凌,校园欺凌")
        self.assertEqual(payload["newOriCharaSubclassNo"], "")
        self.assertEqual(payload["newCharaSubclassNo"], "")
        self.assertEqual(payload["orderByColumn"], "callTime")
        self.assertEqual(payload["pageNum"], "2")

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

    def test_fetch_province_minor_case_rows_uses_province_client(self) -> None:
        with patch.object(wcnr_case_list_dao, "CASE_LIST_PAGE_SIZE", 2), patch(
            "hqzcsj.dao.wcnr_case_list_dao.province_api_client.get_case_list",
            return_value={"code": 0, "total": 1, "rows": [{"caseNo": "P1"}]},
        ) as mock_get_case_list:
            rows = wcnr_case_list_dao.fetch_province_minor_case_rows(
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
            )

        self.assertEqual(rows, [{"caseNo": "P1"}])
        payload = mock_get_case_list.call_args.args[0]
        self.assertEqual(payload["params[startTime]"], "2026-01-01 00:00:00")
        self.assertEqual(payload["params[endTime]"], "2026-01-02 00:00:00")
        self.assertEqual(payload["charaNo"], "01,02")
        self.assertEqual(payload["caseMarkNo"], "01020201,0102020101,0102020102,0102020103")

    def test_fetch_campus_bullying_case_rows_uses_campus_payload(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_case_list_dao.api_client.get_case_list",
            return_value={"code": 0, "total": 1, "rows": [{"caseNo": "JQ001"}]},
        ) as mock_get_case_list:
            rows = wcnr_case_list_dao.fetch_campus_bullying_case_rows(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-30 23:59:59",
            )

        self.assertEqual(rows, [{"caseNo": "JQ001"}])
        payload = mock_get_case_list.call_args.args[0]
        self.assertEqual(payload["caseMarkNo"], "03010108,0604")
        self.assertEqual(payload["caseMark"], "校园欺凌,校园欺凌")

    def test_fetch_cases_by_incident_numbers_groups_by_jqbh(self) -> None:
        conn = _FakeConnection(
            rows=[
                ("JQ001", "AJ002", "案件2", "2026-04-03 00:00:00"),
                ("JQ001", "AJ001", "案件1", "2026-04-02 00:00:00"),
                ("JQ002", "AJ003", "案件3", "2026-04-04 00:00:00"),
            ]
        )

        grouped = wcnr_case_list_dao.fetch_cases_by_incident_numbers(
            conn,
            ["JQ001", "JQ001", "", "JQ002"],
        )

        sql, params = conn.cursor_obj.executed[-1]
        self.assertIn("ajxx_jqbh = ANY(%s)", sql)
        self.assertEqual(params, (["JQ001", "JQ002"],))
        self.assertEqual([row["ajxx_ajbh"] for row in grouped["JQ001"]], ["AJ002", "AJ001"])
        self.assertEqual(grouped["JQ002"][0]["ajxx_ajmc"], "案件3")

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
            {"caseNo": "A4", "charaNo": "01010101"},
        ]

        all_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(rows, subclass_codes=None)
        single_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(
            rows, subclass_codes=["02020201"]
        )
        multi_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(
            rows, subclass_codes=["02020201", "01020301", "01010101"]
        )
        empty_rows = wcnr_case_list_dao.filter_minor_case_rows_by_subclasses(rows, subclass_codes=[])

        self.assertEqual([row["caseNo"] for row in all_rows], ["A1", "A2", "A4"])
        self.assertEqual([row["caseNo"] for row in single_rows], ["A1"])
        self.assertEqual([row["caseNo"] for row in multi_rows], ["A1", "A2", "A4"])
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
                "alarmTime": "2026-01-01 08:00:00",
                "dutydeptname": "单位2",
                "uploadAreaName": "分局2",
                "occuraddress": "地址2",
                "casecontents": "内容2",
                "supplementCaseContents": "处警2",
                "casemark": "未成年人（受害方）",
                "lngOfCall": "112",
                "latOfCall": "23",
                "areaNo": "445303",
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
