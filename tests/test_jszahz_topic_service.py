import io
import unittest
from datetime import datetime
from unittest.mock import patch

from openpyxl import Workbook

from jszahzyj.service import jszahz_topic_service


def _build_workbook_bytes() -> io.BytesIO:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "汇总"
    sheet["A2"] = "姓名"
    sheet["B2"] = "服药情况"
    sheet["C2"] = "监护情况"
    sheet["D2"] = "既往有自杀或严重伤人"
    sheet["E2"] = "身份证号"

    sheet["B4"] = "不规律服药"
    sheet["C4"] = "弱监护"
    sheet["D4"] = "是"
    sheet["E4"] = "440123199001011111"

    sheet["B5"] = "不规律服药"
    sheet["C5"] = "监护良好"
    sheet["D5"] = "无"
    sheet["E5"] = "440123199001011111"

    sheet["B6"] = "规律服药"
    sheet["C6"] = "无监护"
    sheet["D6"] = "无"
    sheet["E6"] = "440123199001011112"

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
        self.assertIn(("440123199001011111", "不规律服药"), labels)
        self.assertIn(("440123199001011111", "弱监护"), labels)
        self.assertIn(("440123199001011111", "既往有严重自杀或伤人行为"), labels)
        self.assertIn(("440123199001011112", "无监护"), labels)

    def test_default_time_range_uses_midnight_window(self) -> None:
        start_time, end_time = jszahz_topic_service.default_time_range()
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        self.assertEqual(end_dt.hour, 0)
        self.assertEqual(end_dt.minute, 0)
        self.assertEqual(end_dt.second, 0)
        self.assertEqual((end_dt - start_dt).days, 7)

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


if __name__ == "__main__":
    unittest.main()
