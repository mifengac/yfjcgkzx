import unittest
from unittest.mock import patch

from jszahzyj.service import jszahz_topic_relation_service


class TestJszahzTopicRelationService(unittest.TestCase):
    def test_attach_relation_counts_merges_all_relation_columns(self) -> None:
        records = [
            {
                "姓名": "张三",
                "身份证号": "440123199001011111",
                "列管时间": "2026-04-01 08:00:00",
            }
        ]
        count_maps = {
            "case": {"440123199001011111": 2},
            "alarm": {"440123199001011111": 1},
            "vehicle": {},
            "video": {"440123199001011111": 4},
            "clinic": {"440123199001011111": 3},
        }
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value=count_maps,
        ) as mock_query:
            enhanced = jszahz_topic_relation_service.attach_relation_counts(records)

        self.assertEqual(enhanced[0]["关联案件"], 2)
        self.assertEqual(enhanced[0]["关联警情"], 1)
        self.assertEqual(enhanced[0]["关联机动车"], 0)
        self.assertEqual(enhanced[0]["关联视频云"], 4)
        self.assertEqual(enhanced[0]["关联门诊"], 3)
        mock_query.assert_called_once_with(["440123199001011111"])

    def test_attach_relation_counts_deduplicates_id_cards_before_batch_query(self) -> None:
        records = [
            {"姓名": "张三", "身份证号": "440123199001011111"},
            {"姓名": "张三-重复", "身份证号": "440123199001011111"},
        ]
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value={"case": {}, "alarm": {}, "vehicle": {}, "video": {}, "clinic": {}},
        ) as mock_query:
            jszahz_topic_relation_service.attach_relation_counts(records)

        mock_query.assert_called_once_with(["440123199001011111"])

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
