import unittest
from unittest.mock import patch

from jszahzyj.service import jszahz_topic_relation_service


class TestJszahzTopicRelationService(unittest.TestCase):
    def test_build_relation_count_payload_normalizes_ids(self) -> None:
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value={"alarm": {"440123199001011111": 2}},
        ) as mock_query:
            payload = jszahz_topic_relation_service.build_relation_count_payload(
                ["440123199001011111", " 440123199001011111 ", ""]
            )

        self.assertEqual(payload["alarm"]["440123199001011111"], 2)
        mock_query.assert_called_once_with(["440123199001011111"], relation_types=list(jszahz_topic_relation_service.RELATION_TYPES.keys()))

    def test_build_relation_count_payload_can_limit_relation_types(self) -> None:
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value={"alarm": {"440123199001011111": 2}},
        ) as mock_query:
            payload = jszahz_topic_relation_service.build_relation_count_payload(
                ["440123199001011111"],
                relation_types=["alarm"],
            )

        self.assertEqual(payload, {"alarm": {"440123199001011111": 2}})
        mock_query.assert_called_once_with(["440123199001011111"], relation_types=["alarm"])

    def test_attach_relation_counts_sets_all_relation_values(self) -> None:
        records = [
            {
                "姓名": "张三",
                "身份证号": "440123199001011111",
                "列管时间": "2026-04-01 08:00:00",
            }
        ]
        count_maps = {
            "case": {"440123199001011111": 1},
            "alarm": {"440123199001011111": 2},
            "vehicle": {"440123199001011111": 3},
            "video": {"440123199001011111": 4},
            "clinic": {"440123199001011111": 5},
            "racing": {"440123199001011111": 6},
        }

        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value=count_maps,
        ) as mock_query:
            enhanced = jszahz_topic_relation_service.attach_relation_counts(records)

        self.assertEqual(enhanced[0]["关联案件"], 1)
        self.assertEqual(enhanced[0]["关联警情"], 2)
        self.assertEqual(enhanced[0]["关联机动车"], 3)
        self.assertEqual(enhanced[0]["关联视频云"], 4)
        self.assertEqual(enhanced[0]["关联门诊"], 5)
        self.assertEqual(enhanced[0]["关联飙车炸街"], 6)
        mock_query.assert_called_once_with(
            ["440123199001011111"],
            relation_types=list(jszahz_topic_relation_service.RELATION_TYPES.keys()),
        )

    def test_attach_relation_counts_defaults_to_zero(self) -> None:
        records = [{"身份证号": "440123199001011111"}]
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_relation_count_maps",
            return_value={},
        ):
            enhanced = jszahz_topic_relation_service.attach_relation_counts(records)

        self.assertEqual(enhanced[0]["关联飙车炸街"], 0)

    def test_initialize_relation_placeholders_sets_none_columns(self) -> None:
        records = [{"身份证号": "440123199001011111", "姓名": "张三"}]
        initialized = jszahz_topic_relation_service.initialize_relation_placeholders(records)

        self.assertIsNone(initialized[0]["关联案件"])
        self.assertIsNone(initialized[0]["关联警情"])
        self.assertIsNone(initialized[0]["关联飙车炸街"])

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

    def test_build_relation_page_payload_supports_racing(self) -> None:
        with patch.object(
            jszahz_topic_relation_service.jszahz_topic_relation_dao,
            "query_racing_rows",
            return_value=[{"文书编号": "WS001"}],
        ):
            payload = jszahz_topic_relation_service.build_relation_page_payload(
                relation_type="racing",
                zjhm="440123199001011111",
                xm="张三",
            )

        self.assertEqual(payload["title"], "关联飙车炸街明细")
        self.assertEqual(payload["records"][0]["文书编号"], "WS001")

    def test_build_relation_page_payload_rejects_invalid_type(self) -> None:
        with self.assertRaises(ValueError):
            jszahz_topic_relation_service.build_relation_page_payload(
                relation_type="invalid",
                zjhm="440123199001011111",
                xm="张三",
            )

    def test_normalize_relation_types_rejects_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            jszahz_topic_relation_service.normalize_relation_types(["case", "bad"])


if __name__ == "__main__":
    unittest.main()
