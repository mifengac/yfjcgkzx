import unittest
from unittest.mock import patch

from jszahzyj.dao import jszahz_topic_relation_dao


class TestJszahzTopicRelationDao(unittest.TestCase):
    def test_query_relation_count_maps_uses_optimized_alarm_and_video_sql(self) -> None:
        side_effect = [
            [],
            [{"身份证号": "440123199001011111", "数量": 2}],
            [],
            [{"身份证号": "440123199001011111", "数量": 1}],
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
        self.assertEqual(mock_execute.call_count, 6)

        alarm_sql, alarm_params = mock_execute.call_args_list[1][0]
        self.assertIn("LIKE ANY", alarm_sql)
        self.assertIn("candidate", alarm_sql)
        self.assertEqual(alarm_params, (["440123199001011111"],))

        video_sql, video_params = mock_execute.call_args_list[3][0]
        self.assertIn('"id_number" = ANY', video_sql)
        self.assertEqual(video_params, (["440123199001011111"],))


if __name__ == "__main__":
    unittest.main()
