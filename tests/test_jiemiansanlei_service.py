import unittest
from unittest.mock import patch

from xunfang.service import jiemiansanlei_service as service


class TestJiemiansanleiService(unittest.TestCase):
    def test_query_classified_filters_street_rows_before_paging(self) -> None:
        rows = [
            {
                "leixing": "盗窃",
                "source": "原始",
                "bureau": "云城分局",
                "station_no": "001",
                "station_name": "A所",
                "call_time": "2026-03-01 10:00:00",
                "address": "地址1",
                "lng": "113.1",
                "lat": "22.1",
                "case_type_name": "盗窃",
                "pred_label": "街面与公共区域",
                "pred_prob": 0.9,
            },
            {
                "leixing": "盗窃",
                "source": "原始",
                "bureau": "云城分局",
                "station_no": "002",
                "station_name": "B所",
                "call_time": "2026-03-01 11:00:00",
                "address": "地址2",
                "lng": "113.2",
                "lat": "22.2",
                "case_type_name": "盗窃",
                "pred_label": "住宅小区",
                "pred_prob": 0.8,
            },
            {
                "leixing": "盗窃",
                "source": "原始",
                "bureau": "云城分局",
                "station_no": "003",
                "station_name": "C所",
                "call_time": "2026-03-01 12:00:00",
                "address": "地址3",
                "lng": "113.3",
                "lat": "22.3",
                "case_type_name": "盗窃",
                "pred_label": "街面与公共区域",
                "pred_prob": 0.7,
            },
        ]

        with patch.object(service, "_fetch_rows_for_filters", return_value=rows), patch.object(
            service, "_append_predictions", side_effect=lambda _rows: None
        ):
            result = service.query_classified(
                start_time="2026-03-01 00:00:00",
                end_time="2026-03-02 00:00:00",
                leixing_list=["盗窃"],
                source_list=["原始"],
                page=2,
                page_size=1,
                street_only=True,
                minor_only=False,
            )

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["page"], 2)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["分类结果"], "街面与公共区域")

    def test_build_case_payload_uses_minor_case_mark_and_original_codes(self) -> None:
        payload = service._build_case_payload(  # noqa: SLF001
            start_time="2026-03-01 00:00:00",
            end_time="2026-03-02 00:00:00",
            source="原始",
            code_csv="0101,0102",
            minor_only=True,
            page_num=1,
            page_size=200,
        )

        self.assertEqual(payload["newOriCharaSubclassNo"], "0101,0102")
        self.assertEqual(payload["newCharaSubclassNo"], "")
        self.assertEqual(payload["caseMarkNo"], service.MINOR_CASE_MARK_NO)

    def test_fetch_source_rows_uses_confirmed_codes_and_matches_leixing(self) -> None:
        with patch.object(
            service.api_client,
            "get_case_list",
            return_value={
                "code": 0,
                "total": 1,
                "rows": [
                    {
                        "caseNo": "A001",
                        "newCharaSubclassNo": "0201,0202",
                        "cmdName": "云安分局",
                        "dutyDeptNo": "1001",
                        "dutyDeptName": "巡逻队",
                        "callTime": "2026-03-01 12:00:00",
                        "occurAddress": "测试地址",
                    }
                ],
            },
        ) as mock_case_list:
            rows = service._fetch_source_rows(  # noqa: SLF001
                start_time="2026-03-01 00:00:00",
                end_time="2026-03-02 00:00:00",
                source="确认",
                leixing="侵财类",
                code_list=["0201", "0301"],
                minor_only=True,
            )

        payload = mock_case_list.call_args.args[0]
        self.assertEqual(payload["newCharaSubclassNo"], "0201,0301")
        self.assertEqual(payload["newOriCharaSubclassNo"], "")
        self.assertEqual(payload["caseMarkNo"], service.MINOR_CASE_MARK_NO)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["leixing"], "侵财类")
        self.assertEqual(rows[0]["bureau"], "云安分局")


if __name__ == "__main__":
    unittest.main()
