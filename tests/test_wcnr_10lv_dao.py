import unittest
from unittest.mock import patch

from hqzcsj.dao.wcnr_10lv_dao import (
    _count_graduate_reoffend_by_region,
    _fetch_graduate_reoffend,
    _fetch_yzbl_ratio_rows,
    _fetch_zmjz_ratio_rows,
    _is_zmjz_ratio_den_row,
    _is_zmjz_ratio_num_row,
    _merge_case_rows_by_type,
    _normalize_person_name_sql,
    fetch_metric_detail_rows,
    fetch_period_data,
)


QUALIFIED = "\u662f\u5426\u7b26\u5408\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2"
APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2\u7533\u8bf7\u4e66"
LEGACY_APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8\u6559\u80b2\u7533\u8bf7\u4e66"


class _FakeCursor:
    def __init__(self, row=None, description=None, rows=None):
        self._row = row
        self.description = description or [
            ("xyrxx_sfzh",),
            ("xyrxx_xm",),
            ("\u5730\u533a\u4ee3\u7801",),
        ]
        self._rows = rows or []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, row=None, description=None, rows=None):
        self.cursors = []
        self._row = row
        self._description = description
        self._rows = rows

    def cursor(self):
        cursor = _FakeCursor(
            row=self._row,
            description=self._description,
            rows=self._rows,
        )
        self.cursors.append(cursor)
        return cursor


class TestWcnr10lvDao(unittest.TestCase):
    def test_normalize_person_name_sql_removes_all_whitespace(self) -> None:
        self.assertEqual(
            _normalize_person_name_sql("demo_col"),
            "REGEXP_REPLACE(COALESCE(demo_col, ''), '[[:space:]\u3000]+', '', 'g')",
        )

    def test_zmjz_ratio_den_requires_qualified_flag(self) -> None:
        self.assertTrue(_is_zmjz_ratio_den_row({QUALIFIED: "\u662f"}))
        self.assertFalse(_is_zmjz_ratio_den_row({QUALIFIED: "\u5426"}))

    def test_zmjz_ratio_num_requires_qualified_and_apply_flags(self) -> None:
        self.assertFalse(_is_zmjz_ratio_num_row({QUALIFIED: "\u5426", APPLY: "\u662f"}))
        self.assertTrue(_is_zmjz_ratio_num_row({QUALIFIED: "\u662f", APPLY: "\u662f"}))
        self.assertFalse(_is_zmjz_ratio_num_row({QUALIFIED: "\u662f", APPLY: "\u5426"}))

    def test_zmjz_ratio_num_accepts_legacy_apply_field_name(self) -> None:
        self.assertTrue(_is_zmjz_ratio_num_row({QUALIFIED: "\u662f", LEGACY_APPLY: "\u662f"}))

    def test_jq_detail_still_uses_shared_jingqing_detail_loader(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["\u6253\u67b6\u6597\u6bb4"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=(
                [{"\u5730\u533a": "445302", "\u62a5\u8b66\u65f6\u95f4": "2026-01-01 00:00:00"}],
                False,
            ),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="jq",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["\u6253\u67b6\u6597\u6bb4"],
            )

        self.assertEqual(rows[0]["\u5730\u533a"], "\u4e91\u57ce")
        self.assertEqual(rows[0]["\u5730\u533a\u4ee3\u7801"], "445302")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "\u8b66\u60c5")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_za_rate_denominator_uses_shared_jingqing_detail_loader(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["\u6253\u67b6\u6597\u6bb4"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=(
                [{"\u5730\u533a": "445302", "\u62a5\u8b66\u65f6\u95f4": "2026-01-01 00:00:00"}],
                False,
            ),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="za_rate",
                part="denominator",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["\u6253\u67b6\u6597\u6bb4"],
            )

        self.assertEqual(rows[0]["\u5730\u533a"], "\u4e91\u57ce")
        self.assertEqual(rows[0]["\u5730\u533a\u4ee3\u7801"], "445302")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "\u8b66\u60c5")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_jq_changsuo_detail_filters_replies_keywords(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["\u6253\u67b6\u6597\u6bb4"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_detail_rows",
            return_value=(
                [
                    {
                        "\u5730\u533a": "445302",
                        "\u62a5\u8b66\u65f6\u95f4": "2026-01-01 00:00:00",
                        "\u5904\u8b66\u60c5\u51b5": "\u591c\u603b\u4f1a\u805a\u96c6",
                    },
                    {
                        "\u5730\u533a": "445303",
                        "\u62a5\u8b66\u65f6\u95f4": "2026-01-02 00:00:00",
                        "\u5904\u8b66\u60c5\u51b5": "\u666e\u901a\u5904\u8b66",
                    },
                ],
                False,
            ),
        ) as mock_fetch_detail_rows:
            rows = fetch_metric_detail_rows(
                object(),
                metric="jq_changsuo",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["\u6253\u67b6\u6597\u6bb4"],
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["\u5730\u533a"], "\u4e91\u57ce")
        self.assertEqual(rows[0]["\u5730\u533a\u4ee3\u7801"], "445302")
        self.assertEqual(rows[0]["\u5904\u8b66\u60c5\u51b5"], "\u591c\u603b\u4f1a\u805a\u96c6")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["metric"], "\u8b66\u60c5")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["diqu"], "__ALL__")
        self.assertEqual(mock_fetch_detail_rows.call_args.kwargs["limit"], 0)

    def test_aj_changsuo_detail_merges_cs_bqh_rows_by_case_no(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["\u6253\u67b6\u6597\u6bb4"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_jq_aj_dao.fetch_ay_patterns",
            return_value=[".*"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_wcnr_ajxx_changsuo_base_rows",
            return_value=[
                {
                    "\u5730\u533a": "445303",
                    "\u6848\u4ef6\u7f16\u53f7": "A1",
                    "\u7b80\u8981\u6848\u60c5": "KTV\u5185\u53d1\u751f\u4e89\u6267",
                },
                {
                    "\u5730\u533a": "445302",
                    "\u6848\u4ef6\u7f16\u53f7": "A2",
                    "\u7b80\u8981\u6848\u60c5": "\u666e\u901a\u6cbb\u5b89\u6848\u4ef6",
                },
            ],
        ) as mock_fetch_rows, patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_wcnr_shr_ajxx_base_rows",
            return_value=[
                {
                    "\u5730\u533a": "445303",
                    "\u6848\u4ef6\u7f16\u53f7": "A1",
                    "\u7b80\u8981\u6848\u60c5": "KTV\u5185\u88ab\u4fb5\u5bb3",
                },
                {
                    "\u5730\u533a": "445302",
                    "\u6848\u4ef6\u7f16\u53f7": "A3",
                    "\u7b80\u8981\u6848\u60c5": "\u68cb\u724c\u5ba4\u5185\u88ab\u4fb5\u5bb3",
                },
            ],
        ):
            rows = fetch_metric_detail_rows(
                object(),
                metric="aj_changsuo",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["\u6253\u67b6\u6597\u6bb4"],
            )

        self.assertEqual([row["\u6848\u4ef6\u7f16\u53f7"] for row in rows], ["A1", "A3"])
        self.assertEqual(rows[0]["\u5730\u533a"], "\u4e91\u5b89")
        self.assertEqual(rows[0]["\u5730\u533a\u4ee3\u7801"], "445303")
        self.assertEqual(rows[1]["\u5730\u533a"], "\u4e91\u57ce")
        self.assertEqual(rows[1]["\u5730\u533a\u4ee3\u7801"], "445302")
        self.assertEqual(mock_fetch_rows.call_args.kwargs["patterns"], [".*"])
        self.assertIsNone(mock_fetch_rows.call_args.kwargs["diqu"])

    def test_merge_case_rows_by_type_splits_bqh_rows_and_dedupes_case_no(self) -> None:
        rows_by_type = _merge_case_rows_by_type(
            [
                {"地区": "445302", "案件编号": "A1", "案件类型": "行政", "案件名称": "嫌疑人行政"},
                {"地区": "445303", "案件编号": "B1", "案件类型": "刑事", "案件名称": "嫌疑人刑事"},
            ],
            [
                {"地区": "445302", "案件编号": "A1", "案件类型": "行政", "案件名称": "被侵害行政"},
                {"地区": "445321", "案件编号": "A2", "案件类型": "行政", "案件名称": "被侵害行政2"},
                {"地区": "445303", "案件编号": "B2", "案件类型": "刑事", "案件名称": "被侵害刑事"},
            ],
        )

        self.assertEqual([row["案件编号"] for row in rows_by_type["行政"]], ["A1", "A2"])
        self.assertEqual([row["案件编号"] for row in rows_by_type["刑事"]], ["B1", "B2"])
        self.assertEqual(rows_by_type["行政"][0]["来源字段"], "嫌疑人、被侵害")
        self.assertEqual(rows_by_type["行政"][1]["来源字段"], "被侵害")
        self.assertEqual(rows_by_type["刑事"][0]["来源字段"], "嫌疑人")

    def test_xingzheng_detail_merges_bqh_admin_cases_with_source_field(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["打架斗殴"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_jq_aj_dao.fetch_ay_patterns",
            return_value=[".*"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_wcnr_ajxx_changsuo_base_rows",
            return_value=[
                {"地区": "445302", "案件编号": "A1", "案件类型": "行政"},
                {"地区": "445303", "案件编号": "B1", "案件类型": "刑事"},
            ],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.fetch_wcnr_shr_ajxx_base_rows",
            return_value=[
                {"地区": "445302", "案件编号": "A1", "案件类型": "行政"},
                {"地区": "445321", "案件编号": "A2", "案件类型": "行政"},
                {"地区": "445303", "案件编号": "B2", "案件类型": "刑事"},
            ],
        ):
            rows = fetch_metric_detail_rows(
                object(),
                metric="xingzheng",
                part="value",
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["打架斗殴"],
            )

        self.assertEqual([row["案件编号"] for row in rows], ["A1", "A2"])
        self.assertEqual(rows[0]["来源字段"], "嫌疑人、被侵害")
        self.assertEqual(rows[1]["来源字段"], "被侵害")
        self.assertEqual(rows[0]["地区"], "云城")
        self.assertEqual(rows[1]["地区"], "新兴")

    def test_fetch_zmjz_ratio_rows_falls_back_when_view_lacks_name_normalization(self) -> None:
        conn = _FakeConnection()

        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._relation_exists",
            return_value=True,
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._view_uses_normalized_name_matching",
            return_value=False,
        ):
            rows = _fetch_zmjz_ratio_rows(
                conn,
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
            )

        self.assertEqual(rows, [])
        sql, params = conn.cursors[-1].executed[-1]
        self.assertNotIn("v_wcnr_zmjz_ratio_base", sql)
        self.assertIn(
            "REGEXP_REPLACE(COALESCE(t.\"xgry_xm\", ''), '[[:space:]\u3000]+', '', 'g') = "
            "REGEXP_REPLACE(COALESCE(q.\"xyrxx_xm\", ''), '[[:space:]\u3000]+', '', 'g')",
            sql,
        )
        self.assertEqual(params[:2], ["2026-01-01 00:00:00", "2026-01-02 00:00:00"])

    def test_fetch_yzbl_ratio_rows_prefers_base2_view(self) -> None:
        conn = _FakeConnection()

        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._relation_exists",
            return_value=True,
        ) as mock_relation_exists:
            rows = _fetch_yzbl_ratio_rows(
                conn,
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
            )

        self.assertEqual(rows, [])
        sql, params = conn.cursors[-1].executed[-1]
        self.assertIn('FROM "ywdata"."v_wcnr_yzbl_ratio_base2" src', sql)
        self.assertEqual(mock_relation_exists.call_args.kwargs["name"], "v_wcnr_yzbl_ratio_base2")
        self.assertEqual(params[:2], ["2026-01-01 00:00:00", "2026-01-02 00:00:00"])

    def test_fetch_yzbl_ratio_rows_falls_back_to_legacy_view(self) -> None:
        conn = _FakeConnection()

        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._relation_exists",
            side_effect=[False, True],
        ) as mock_relation_exists:
            _fetch_yzbl_ratio_rows(
                conn,
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
            )

        sql, _params = conn.cursors[-1].executed[-1]
        self.assertIn('FROM "ywdata"."v_wcnr_yzbl_ratio_base" src', sql)
        self.assertEqual(
            [call.kwargs["name"] for call in mock_relation_exists.call_args_list],
            ["v_wcnr_yzbl_ratio_base2", "v_wcnr_yzbl_ratio_base"],
        )

    def test_fetch_yzbl_ratio_rows_live_fallback_uses_jbxx_base(self) -> None:
        conn = _FakeConnection()

        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._relation_exists",
            return_value=False,
        ):
            _fetch_yzbl_ratio_rows(
                conn,
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
            )

        sql, _params = conn.cursors[-1].executed[-1]
        self.assertIn('FROM "ywdata"."v_wcnr_wfry_jbxx_base" b', sql)
        self.assertIn("FROM wfzf_people v", sql)
        self.assertNotIn('FROM "ywdata"."v_wcnr_wfry_jbxx" v', sql)

    def test_fetch_graduate_reoffend_uses_latest_wfry_base_record(self) -> None:
        conn = _FakeConnection()

        rows = _fetch_graduate_reoffend(
            conn,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-30 23:59:59",
            leixing_list=["盗窃"],
            jz_time_lt6=False,
            xingshi_only=False,
            minor_only=False,
        )

        self.assertEqual(rows, [])
        sql, params = conn.cursors[-1].executed[-1]
        self.assertIn('FROM "ywdata"."v_wcnr_wfry_jbxx_base" w', sql)
        self.assertIn('SELECT DISTINCT ON (w."xyrxx_sfzh")', sql)
        self.assertIn('w."ajxx_join_ajxx_lasj" DESC NULLS LAST', sql)
        self.assertIn('JOIN latest_wfry x', sql)
        self.assertIn('x."ajxx_join_ajxx_lasj" > g."离校时间_raw"', sql)
        self.assertIn('COALESCE(x."ajxx_join_ajxx_ay", \'\') SIMILAR TO ctc."ay_pattern"', sql)
        self.assertNotIn('JOIN "ywdata"."zq_zfba_wcnr_xyr" x', sql)
        self.assertEqual(params, ["2026-04-01 00:00:00", "2026-04-30 23:59:59", ["盗窃"]])

    def test_count_graduate_reoffend_uses_latest_wfry_base_record(self) -> None:
        conn = _FakeConnection()

        counts = _count_graduate_reoffend_by_region(
            conn,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-30 23:59:59",
            leixing_list=[],
            jz_time_lt6=False,
            xingshi_only=False,
            minor_only=False,
        )

        self.assertEqual(counts["__ALL__"], 0)
        sql, params = conn.cursors[-1].executed[-1]
        self.assertIn('FROM "ywdata"."v_wcnr_wfry_jbxx_base" w', sql)
        self.assertIn('SELECT DISTINCT ON (w."xyrxx_sfzh")', sql)
        self.assertIn('JOIN latest_wfry x', sql)
        self.assertIn('x."ajxx_join_ajxx_lasj" > g."离校时间_raw"', sql)
        self.assertNotIn('JOIN "ywdata"."zq_zfba_wcnr_xyr" x', sql)
        self.assertEqual(params, ["2026-04-01 00:00:00", "2026-04-30 23:59:59"])

    def test_fetch_criminal_graduate_reoffend_keeps_legacy_source(self) -> None:
        conn = _FakeConnection()

        _fetch_graduate_reoffend(
            conn,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-30 23:59:59",
            leixing_list=["盗窃"],
            jz_time_lt6=True,
            xingshi_only=True,
            minor_only=True,
        )

        sql, params = conn.cursors[-1].executed[-1]
        self.assertIn('JOIN "ywdata"."zq_zfba_wcnr_xyr" x', sql)
        self.assertIn('COALESCE(x."xyrxx_ay_mc", \'\') SIMILAR TO ctc."ay_pattern"', sql)
        self.assertNotIn('v_wcnr_wfry_jbxx_base', sql)
        self.assertEqual(params, ["2026-04-01 00:00:00", "2026-04-30 23:59:59", ["盗窃"]])

    def test_fetch_period_data_keeps_pattern_dependent_metrics_empty_when_no_patterns_match(self) -> None:
        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=["\u6253\u67b6\u6597\u6bb4"],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_jq_aj_dao.fetch_ay_patterns",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.count_jq_by_diqu",
            return_value={"445302": 2},
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_wcnr_jqaj_dao.count_zhuanan_by_diqu",
            return_value={"445302": 1},
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._load_detail_rows",
            side_effect=[[{"\u5730\u533a\u4ee3\u7801": "445302"}], [{"\u5730\u533a\u4ee3\u7801": "445303"}]],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_wfzf_people_rows",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_yzbl_ratio_rows",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_sx_songjiao_den_rows",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_sx_songjiao_num_rows",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._count_naguan_base_by_region",
            return_value={
                "445300": 0,
                "445302": 5,
                "445303": 0,
                "445381": 0,
                "445321": 0,
                "445322": 0,
                "__ALL__": 5,
            },
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_zljiaqjh_detail_rows",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._fetch_zmjz_ratio_rows",
            side_effect=AssertionError("should not query zmjz ratio rows when patterns are empty"),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._count_graduates_by_region",
            side_effect=AssertionError("should not count graduates when patterns are empty"),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._count_graduate_reoffend_by_region",
            side_effect=AssertionError("should not count graduate reoffend rows when patterns are empty"),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._count_naguan_reoffend_by_region",
            side_effect=AssertionError("should not count naguan reoffend rows when patterns are empty"),
        ):
            payload = fetch_period_data(
                object(),
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=["\u6253\u67b6\u6597\u6bb4"],
                include_details=False,
            )

        counts = payload["counts"]
        self.assertEqual(counts["jq"]["445302"], 2)
        self.assertEqual(counts["jq"]["__ALL__"], 2)
        self.assertEqual(counts["zhuanan"]["445302"], 1)
        self.assertEqual(counts["zhuanan"]["__ALL__"], 1)
        self.assertEqual(counts["jq_changsuo"]["445302"], 1)
        self.assertEqual(counts["jq_changsuo"]["__ALL__"], 1)
        self.assertEqual(counts["aj_changsuo"]["445303"], 1)
        self.assertEqual(counts["aj_changsuo"]["__ALL__"], 1)
        self.assertEqual(counts["xingzheng"]["__ALL__"], 0)
        self.assertEqual(counts["xingshi"]["__ALL__"], 0)
        self.assertEqual(counts["bqh_case"]["__ALL__"], 0)
        self.assertEqual(counts["cs_bqh_case"]["__ALL__"], 0)
        self.assertEqual(counts["zmjz_cover_num"]["__ALL__"], 0)
        self.assertEqual(counts["zmjz_cover_den"]["__ALL__"], 0)
        self.assertEqual(counts["zmy_den"]["__ALL__"], 0)
        self.assertEqual(counts["zmy_num"]["__ALL__"], 0)
        self.assertEqual(counts["zmjz_den"]["__ALL__"], 0)
        self.assertEqual(counts["zmjz_num"]["__ALL__"], 0)
        self.assertEqual(counts["naguan_den"]["445302"], 5)
        self.assertEqual(counts["naguan_den"]["__ALL__"], 5)
        self.assertEqual(counts["naguan_num"]["__ALL__"], 0)
        self.assertEqual(payload["details"], {})

    def test_fetch_period_data_merges_aj_changsuo_counts_by_case_no(self) -> None:
        def _fake_base_counts(*args, **kwargs):
            counts = kwargs["counts"]
            counts["jq"] = {"445302": 2, "__ALL__": 2}
            counts["zhuanan"] = {"445302": 1, "__ALL__": 1}
            counts["jq_changsuo"] = {"445302": 1, "__ALL__": 1}
            return [], [
                {
                    "\u5730\u533a\u4ee3\u7801": "445302",
                    "\u5730\u533a": "\u4e91\u57ce",
                    "\u6848\u4ef6\u7f16\u53f7": "A1",
                },
                {
                    "\u5730\u533a\u4ee3\u7801": "445302",
                    "\u5730\u533a": "\u4e91\u57ce",
                    "\u6848\u4ef6\u7f16\u53f7": "A2",
                },
            ]

        def _fake_case_counts(*args, **kwargs):
            counts = kwargs["counts"]
            counts["bqh_case"] = {"445302": 1, "__ALL__": 1}
            counts["cs_bqh_case"] = {"445302": 1, "445303": 1, "__ALL__": 2}
            return [], [
                {
                    "\u5730\u533a\u4ee3\u7801": "445302",
                    "\u5730\u533a": "\u4e91\u57ce",
                    "\u6848\u4ef6\u7f16\u53f7": "A2",
                },
                {
                    "\u5730\u533a\u4ee3\u7801": "445303",
                    "\u5730\u533a": "\u4e91\u5b89",
                    "\u6848\u4ef6\u7f16\u53f7": "A3",
                },
            ]

        with patch(
            "hqzcsj.dao.wcnr_10lv_dao._normalize_leixing_for_query",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao.zfba_jq_aj_dao.fetch_ay_patterns",
            return_value=[],
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_base_counts_and_place_rows",
            side_effect=_fake_base_counts,
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_case_counts",
            side_effect=_fake_case_counts,
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_zmjz_cover_counts",
            return_value=([], []),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_wfzf_ratio_counts",
            return_value=([], [], [], [], []),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_graduate_counts",
            return_value=([], [], [], []),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_naguan_counts",
            return_value=([], []),
        ), patch(
            "hqzcsj.dao.wcnr_10lv_dao._populate_zljiaqjh_counts",
            return_value=[],
        ):
            payload = fetch_period_data(
                object(),
                start_time="2026-01-01 00:00:00",
                end_time="2026-01-02 00:00:00",
                leixing_list=[],
                include_details=False,
            )

        counts = payload["counts"]
        self.assertEqual(counts["aj_changsuo"]["445302"], 2)
        self.assertEqual(counts["aj_changsuo"]["445303"], 1)
        self.assertEqual(counts["aj_changsuo"]["__ALL__"], 3)
        self.assertEqual(counts["cs_bqh_case"]["__ALL__"], 2)


if __name__ == "__main__":
    unittest.main()
