import unittest
from unittest.mock import patch

from jszahzyj.service import jszahz_topic_relation_service


class TestJszahzTopicRelationService(unittest.TestCase):
    def test_append_relation_columns_sets_all_relation_placeholders(self) -> None:
        records = [
            {
                "姓名": "张三",
                "身份证号": "440123199001011111",
                "列管时间": "2026-04-01 08:00:00",
            }
        ]

        enhanced = jszahz_topic_relation_service.append_relation_columns(records)

        self.assertIsNone(enhanced[0]["关联案件"])
        self.assertIsNone(enhanced[0]["关联警情"])
        self.assertIsNone(enhanced[0]["关联机动车"])
        self.assertIsNone(enhanced[0]["关联视频云"])
        self.assertIsNone(enhanced[0]["关联门诊"])

    def test_build_relation_page_payload_returns_empty_message(self) -> None:
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_case_rows",
            return_value=[],
        ):
            payload = jszahz_topic_relation_service.build_relation_page_payload(
                relation_type="case",
                zjhm="440123199001011111",
                xm="张三",
            )

        self.assertEqual(payload["title"], "关联案件明细")
        self.assertEqual(payload["message"], "未查询到该人员的关联案件数据")
        self.assertEqual(payload["records"], [])

    def test_build_relation_page_payload_rejects_invalid_type(self) -> None:
        with self.assertRaises(ValueError):
            jszahz_topic_relation_service.build_relation_page_payload(
                relation_type="invalid",
                zjhm="440123199001011111",
                xm="张三",
            )


if __name__ == "__main__":
    unittest.main()
