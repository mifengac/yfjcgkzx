import unittest
from unittest.mock import patch

from jszahzyj.dao import jszahz_topic_relation_dao


class TestJszahzTopicRelationDao(unittest.TestCase):
    def test_query_relation_count_maps_uses_alarm_mapping_table_and_video_index_sql(self) -> None:
        side_effect = [
            [],
            [{"身份证号": "440123199001011111", "数量": 2}],
            [],
            [{"身份证号": "440123199001011111", "数量": 1}],
            [],
            [],
            [],
        ]

        with patch(
            "jszahzyj.dao.jszahz_topic_relation_dao.execute_query",
            side_effect=side_effect,
        ) as mock_execute:
            result = jszahz_topic_relation_dao.query_relation_count_maps(
                ["440123199001011111", "440123199001011111"]
            )

        self.assertEqual(result["alarm"]["440123199001011111"], 2)
        self.assertEqual(result["video"]["440123199001011111"], 1)
        self.assertEqual(mock_execute.call_count, 7)

        alarm_sql, alarm_params = mock_execute.call_args_list[1][0]
        self.assertIn('"jcgkzx_monitor"."jszahz_jq_sfzh_map"', alarm_sql)
        self.assertIn('"sfzh" = ANY', alarm_sql)
        self.assertNotIn("LIKE ANY", alarm_sql)
        self.assertEqual(alarm_params, (["440123199001011111"],))

        video_sql, video_params = mock_execute.call_args_list[3][0]
        self.assertIn('"id_number" = ANY', video_sql)
        self.assertEqual(video_params, (["440123199001011111"],))

    def test_query_alarm_rows_reads_from_mapping_table(self) -> None:
        with patch(
            "jszahzyj.dao.jszahz_topic_relation_dao.execute_query",
            return_value=[],
        ) as mock_execute:
            jszahz_topic_relation_dao.query_alarm_rows("440123199001011111")

        alarm_sql, alarm_params = mock_execute.call_args[0]
        self.assertIn('"jcgkzx_monitor"."jszahz_jq_sfzh_map"', alarm_sql)
        self.assertIn('AS "警情地址"', alarm_sql)
        self.assertIn('AS "处警情况"', alarm_sql)
        self.assertIn('AS "警情编号"', alarm_sql)
        self.assertIn('ORDER BY jqm."source_sync_ts" DESC', alarm_sql)
        self.assertEqual(alarm_params, ("440123199001011111",))

    def test_query_relation_detail_rows_batch_uses_any_filter(self) -> None:
        with patch(
            "jszahzyj.dao.jszahz_topic_relation_dao.execute_query",
            return_value=[{"身份证号": "440123199001011111", "案件编号": "AJ001"}],
        ) as mock_execute:
            rows = jszahz_topic_relation_dao.query_relation_detail_rows_batch(
                "case",
                ["440123199001011111", "440123199001011111"],
            )

        self.assertEqual(rows[0]["案件编号"], "AJ001")
        batch_sql, batch_params = mock_execute.call_args[0]
        self.assertIn('"xyrxx_sfzh" = ANY', batch_sql)
        self.assertIn('AS "身份证号"', batch_sql)
        self.assertEqual(batch_params, (["440123199001011111"],))

    def test_query_relation_count_maps_can_limit_relation_types(self) -> None:
        with patch(
            "jszahzyj.dao.jszahz_topic_relation_dao.execute_query",
            side_effect=[[{"身份证号": "440123199001011111", "数量": 2}]],
        ) as mock_execute:
            result = jszahz_topic_relation_dao.query_relation_count_maps(
                ["440123199001011111"],
                relation_types=["alarm"],
            )

        self.assertEqual(result, {"alarm": {"440123199001011111": 2}})
        self.assertEqual(mock_execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
