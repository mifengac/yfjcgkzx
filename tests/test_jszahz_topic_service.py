import io
import unittest
from datetime import datetime
from unittest.mock import patch

from openpyxl import Workbook

from jszahzyj.service import jszahz_topic_service


def _build_workbook_bytes() -> io.BytesIO:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "\u6c47\u603b"

    sheet["A2"] = "\u6240\u5c5e\u53bf(\u5e02)\u533a"
    sheet["B2"] = "\u6d3e\u51fa\u6240"
    sheet["C2"] = "\u59d3\u540d"
    sheet["D2"] = "\u4eba\u5458\u72b6\u6001"
    sheet["E2"] = "\u8eab\u4efd\u8bc1\u53f7\u7801"
    sheet["F2"] = "\u624b\u673a\u53f7\u7801"
    sheet["G2"] = "\u670d\u836f\u60c5\u51b5"
    sheet["H2"] = "\u76d1\u62a4\u60c5\u51b5"
    sheet["I2"] = "\u65e2\u5f80\u6709\u81ea\u6740\u6216\u4e25\u91cd\u4f24\u4eba"
    sheet["J2"] = "\u5217\u4e3a\u91cd\u70b9\u5173\u6ce8\u4eba\u5458"
    sheet["K2"] = "\u4eba\u5458\u6807\u7b7e"
    sheet["L2"] = "\u6807\u7b7e\u6570\u91cf"

    sheet["E4"] = "440123199001011111"
    sheet["G4"] = "\u4e0d\u89c4\u5f8b\u670d\u836f"
    sheet["H4"] = "\u5f31\u76d1\u62a4"
    sheet["I4"] = "\u662f"

    sheet["E5"] = "440123199001011111"
    sheet["G5"] = "\u4e0d\u89c4\u5f8b\u670d\u836f"
    sheet["H5"] = "\u76d1\u62a4\u826f\u597d"
    sheet["I5"] = "\u65e0"

    sheet["E6"] = "440123199001011112"
    sheet["G6"] = "\u89c4\u5f8b\u670d\u836f"
    sheet["H6"] = "\u65e0\u76d1\u62a4"
    sheet["I6"] = "\u65e0"

    data = io.BytesIO()
    workbook.save(data)
    data.seek(0)
    return data


def _build_workbook_with_group_header() -> io.BytesIO:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "\u6c47\u603b"

    sheet["A2"] = "\u6240\u5c5e\u53bf(\u5e02)\u533a"
    sheet["B2"] = "\u6d3e\u51fa\u6240"
    sheet["C2"] = "\u59d3\u540d"
    sheet["D2"] = "\u4eba\u5458\u72b6\u6001"
    sheet["E2"] = "\u8eab\u4efd\u8bc1\u53f7\u7801"
    sheet["F2"] = "\u624b\u673a\u53f7\u7801"
    sheet.merge_cells("G2:J2")
    sheet["G2"] = "\u4eba\u5458\u6807\u7b7e"
    sheet["K2"] = "\u4eba\u5458\u6807\u7b7e"
    sheet["L2"] = "\u6807\u7b7e\u6570\u91cf"

    sheet["G3"] = "\u670d\u836f\u60c5\u51b5"
    sheet["H3"] = "\u76d1\u62a4\u60c5\u51b5"
    sheet["I3"] = "\u65e2\u5f80\u6709\u81ea\u6740\u6216\u4e25\u91cd\u4f24\u4eba"
    sheet["J3"] = "\u5217\u4e3a\u91cd\u70b9\u5173\u6ce8\u4eba\u5458"

    sheet["E4"] = "440123199001011111"
    sheet["G4"] = "\u4e0d\u89c4\u5f8b\u670d\u836f"
    sheet["H4"] = "\u5f31\u76d1\u62a4"
    sheet["I4"] = "\u662f"
    sheet["J4"] = "\u662f"

    data = io.BytesIO()
    workbook.save(data)
    data.seek(0)
    return data


class TestJszahzTopicService(unittest.TestCase):
    def test_parse_person_type_workbook_extracts_exact_labels(self) -> None:
        payload = jszahz_topic_service.parse_person_type_workbook(_build_workbook_bytes())

        labels = {(row["zjhm"], row["person_type"]) for row in payload.rows}
        self.assertEqual(payload.imported_row_count, 3)
        self.assertEqual(payload.generated_tag_count, 4)
        self.assertEqual(payload.tagged_person_count, 2)
        self.assertIn(("440123199001011111", "\u4e0d\u89c4\u5f8b\u670d\u836f"), labels)
        self.assertIn(("440123199001011111", "\u5f31\u76d1\u62a4"), labels)
        self.assertIn(("440123199001011111", "\u65e2\u5f80\u6709\u4e25\u91cd\u81ea\u6740\u6216\u4f24\u4eba\u884c\u4e3a"), labels)
        self.assertIn(("440123199001011112", "\u65e0\u76d1\u62a4"), labels)

    def test_parse_person_type_workbook_ignores_header_merges(self) -> None:
        payload = jszahz_topic_service.parse_person_type_workbook(_build_workbook_with_group_header())

        labels = {(row["zjhm"], row["person_type"]) for row in payload.rows}
        self.assertEqual(payload.imported_row_count, 1)
        self.assertEqual(payload.generated_tag_count, 3)
        self.assertIn(("440123199001011111", "\u4e0d\u89c4\u5f8b\u670d\u836f"), labels)
        self.assertIn(("440123199001011111", "\u5f31\u76d1\u62a4"), labels)
        self.assertIn(("440123199001011111", "\u65e2\u5f80\u6709\u4e25\u91cd\u81ea\u6740\u6216\u4f24\u4eba\u884c\u4e3a"), labels)

    def test_default_time_range_uses_midnight_window(self) -> None:
        start_time, end_time = jszahz_topic_service.default_time_range()
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        self.assertEqual(start_dt, datetime(2025, 1, 1, 0, 0, 0))
        self.assertEqual(end_dt.hour, 0)
        self.assertEqual(end_dt.minute, 0)
        self.assertEqual(end_dt.second, 0)
        self.assertGreaterEqual((end_dt - start_dt).days, 0)

    def test_query_summary_payload_appends_total_row(self) -> None:
        with patch.object(
            jszahz_topic_service.jszahz_topic_dao,
            "get_active_batch",
            return_value={"id": 7, "source_file_name": "demo.xlsx", "created_at": None, "activated_at": None},
        ), patch.object(
            jszahz_topic_service.jszahz_topic_dao,
            "query_summary_rows",
            return_value=[
                {"分局代码": "445302000000", "分局名称": "云城分局", "去重患者数": 2},
                {"分局代码": "445303000000", "分局名称": "云安分局", "去重患者数": 1},
            ],
        ):
            payload = jszahz_topic_service.query_summary_payload(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                branch_codes=[],
                person_types=["弱监护"],
                risk_labels=["1级患者"],
            )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["count"], 3)
        self.assertEqual(payload["records"][-1]["分局名称"], "汇总")
        self.assertEqual(payload["records"][-1]["去重患者数"], 3)

    def test_query_summary_payload_returns_empty_without_active_batch(self) -> None:
        with patch.object(jszahz_topic_service.jszahz_topic_dao, "get_active_batch", return_value=None):
            payload = jszahz_topic_service.query_summary_payload(
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                branch_codes=[],
                person_types=[],
                risk_labels=[],
            )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["records"], [])
        self.assertIn("请先上传", payload["message"])

    def test_query_detail_payload_attaches_relation_counts(self) -> None:
        base_records = [
            {
                "姓名": "张三",
                "身份证号": "440123199001011111",
                "列管时间": "2026-04-01 08:00:00",
                "列管单位": "云城派出所",
            }
        ]
        enhanced_records = [
            {
                **base_records[0],
                "关联案件": 0,
                "关联警情": 1,
                "关联机动车": 2,
                "关联视频云": 3,
                "关联门诊": 4,
                "关联飙车炸街": 5,
            }
        ]
        with patch.object(
            jszahz_topic_service.jszahz_topic_dao,
            "get_active_batch",
            return_value={"id": 5, "source_file_name": "demo.xlsx", "created_at": None, "activated_at": None},
        ), patch.object(
            jszahz_topic_service.jszahz_topic_dao,
            "query_detail_rows",
            return_value=base_records,
        ) as mock_query, patch(
            "jszahzyj.service.jszahz_topic_service.attach_relation_counts",
            return_value=enhanced_records,
        ) as mock_attach:
            payload = jszahz_topic_service.query_detail_payload(
                branch_code="445302000000",
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                person_types=[],
                risk_labels=[],
            )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["records"][0]["关联飙车炸街"], 5)
        mock_query.assert_called_once()
        mock_attach.assert_called_once_with(base_records)


if __name__ == "__main__":
    unittest.main()
