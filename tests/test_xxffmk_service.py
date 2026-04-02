import unittest
from unittest.mock import patch

from xxffmk.service import xxffmk_service as service


class _DummyConnection:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def close(self) -> None:
        self.closed = True
        return None

    def commit(self) -> None:
        self.committed = True
        return None

    def rollback(self) -> None:
        self.rolled_back = True
        return None


def _dimension_result(counts):
    return service.DimensionResult(
        counts_by_school=dict(counts),
        detail_rows_by_school={},
        unmatched_rows=[],
    )


class TestXxffmkService(unittest.TestCase):
    def test_school_matcher_handles_prefixed_road_name(self) -> None:
        matcher = service.SchoolMatcher(
            [
                {"xxbsm": "A001", "xxmc": "AB中学", "source_type": "zxxj"},
            ]
        )

        matched = matcher.match("星河路-AB中学")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["xxbsm"], "A001")

    def test_school_matcher_honors_alias_map(self) -> None:
        with patch.dict(service.SCHOOL_ALIAS_MAP, {"AB实验中学": "AB中学"}, clear=True):
            matcher = service.SchoolMatcher(
                [
                    {"xxbsm": "A001", "xxmc": "AB中学", "source_type": "zxxj"},
                ]
            )
            matched = matcher.match("AB实验中学")

        self.assertIsNotNone(matched)
        self.assertEqual(matched["xxmc"], "AB中学")

    def test_rank_counts_uses_skip_rank_and_score_floor(self) -> None:
        counts = {f"S{i:03d}": max(1, 30 - i) for i in range(1, 15)}
        counts["S001"] = 40
        counts["S002"] = 40

        ranked = service._rank_counts(counts, base_score=10)

        self.assertEqual(ranked["S001"]["rank"], 1)
        self.assertEqual(ranked["S002"]["rank"], 1)
        self.assertEqual(ranked["S003"]["rank"], 3)
        self.assertEqual(ranked["S014"]["score"], 0)

    def test_build_rank_payload_merges_dimension_scores(self) -> None:
        school_info_map = {
            "A": {"xxbsm": "A", "xxmc": "甲学校"},
            "B": {"xxbsm": "B", "xxmc": "乙学校"},
            "C": {"xxbsm": "C", "xxmc": "丙学校"},
        }
        dimension_results = {
            "songsheng": _dimension_result({"A": 10, "B": 10, "C": 1}),
            "jingqing": _dimension_result({"A": 5, "C": 5}),
            "tuanhuo": _dimension_result({"B": 2}),
            "chuoxue": _dimension_result({}),
            "yebuguisu": _dimension_result({"A": 1, "B": 1, "C": 1}),
        }

        with patch.object(service, "get_database_connection", return_value=_DummyConnection()), \
             patch.object(
                 service,
                 "_load_dimension_results",
                 return_value=(None, school_info_map, dimension_results, {"total_seconds": 0.1}),
             ):
            payload = service.build_rank_payload(
                start_time="2026-01-01 00:00:00",
                end_time="2026-03-31 23:59:59",
                limit=10,
            )

        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["rows"][0]["rank"], 1)
        self.assertEqual(payload["rows"][1]["rank"], 1)
        self.assertEqual(payload["rows"][2]["rank"], 3)
        self.assertEqual(payload["rows"][0]["dimension_scores"]["songsheng"]["score"], 20)
        self.assertEqual(payload["rows"][1]["dimension_scores"]["tuanhuo"]["score"], 15)
        self.assertEqual(payload["rows"][2]["dimension_scores"]["songsheng"]["score"], 18)
        self.assertIn("timings", payload)
        self.assertIn("total_seconds", payload["timings"])

    def test_dimension1_detail_returns_raw_rows(self) -> None:
        dummy_conn = _DummyConnection()
        fake_dimension_results = {
            "songsheng": service.DimensionResult(
                counts_by_school={"A": 3},
                detail_rows_by_school={"A": [{"原学校名称": "AB中学", "送生人数": 3}]},
                unmatched_rows=[],
                raw_keys_by_school={"A": ["AB中学"]},
            ),
            "jingqing": _dimension_result({}),
            "tuanhuo": _dimension_result({}),
            "chuoxue": _dimension_result({}),
            "yebuguisu": _dimension_result({}),
        }
        with patch.object(service, "get_database_connection", return_value=dummy_conn), patch.object(
            service,
            "_load_dimension_results",
            return_value=(None, {"A": {"xxbsm": "A", "xxmc": "A校"}}, fake_dimension_results, {"total_seconds": 0.1}),
        ), patch.object(
            service.xxffmk_dao,
            "fetch_dimension1_detail_rows",
            return_value=[
                {"rx_time": "2026-01-01 09:00:00", "yxx": "AB中学", "sfzjh": "123", "xm": "张三"},
                {"rx_time": "2026-01-02 09:00:00", "yxx": "AB中学", "sfzjh": "456", "xm": "李四"},
            ],
        ):
            payload = service.get_dimension_detail(
                dimension="songsheng",
                xxbsm="A",
                start_time="2026-01-01 00:00:00",
                end_time="2026-03-31 23:59:59",
                page=1,
                page_size=20,
            )

        self.assertEqual(payload["total"], 2)
        self.assertIn("rx_time", payload["columns"])
        self.assertEqual(payload["rows"][0]["yxx"], "AB中学")

    def test_refresh_materialized_views_returns_summary(self) -> None:
        dummy_conn = _DummyConnection()
        with patch.object(service, "get_database_connection", return_value=dummy_conn), patch.object(
            service.xxffmk_dao,
            "refresh_materialized_views",
            return_value=[
                'REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_school_dim"',
                'REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_student_school_rel"',
                'REFRESH MATERIALIZED VIEW "ywdata"."mv_xxffmk_dim5_night_day"',
            ],
        ):
            payload = service.refresh_materialized_views()

        self.assertTrue(dummy_conn.committed)
        self.assertTrue(dummy_conn.closed)
        self.assertEqual(payload["refreshed_count"], 3)
        self.assertIn('"ywdata"."mv_xxffmk_dim5_night_day"', payload["refreshed_views"])
        self.assertIn("已刷新 3 个物化视图", payload["message"])


if __name__ == "__main__":
    unittest.main()
