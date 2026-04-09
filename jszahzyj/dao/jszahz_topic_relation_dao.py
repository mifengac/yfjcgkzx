from __future__ import annotations

from typing import Any, Dict, Iterable, List

from gonggong.config.database import execute_query


def _normalize_zjhms(zjhms: Iterable[str]) -> List[str]:
    seen = set()
    normalized: List[str] = []
    for value in zjhms or []:
        text = str(value or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _query_count_map(sql: str, params: tuple[Any, ...]) -> Dict[str, int]:
    rows = execute_query(sql, params)
    counts: Dict[str, int] = {}
    for row in rows:
        zjhm = str(row.get("身份证号") or "").strip().upper()
        if not zjhm:
            continue
        counts[zjhm] = int(row.get("数量") or 0)
    return counts


def query_relation_count_maps(zjhms: Iterable[str]) -> Dict[str, Dict[str, int]]:
    normalized = _normalize_zjhms(zjhms)
    empty = {
        "case": {},
        "alarm": {},
        "vehicle": {},
        "video": {},
        "clinic": {},
        "racing": {},
    }
    if not normalized:
        return empty

    return {
        "case": _query_count_map(
            """
            WITH ids AS (
                SELECT DISTINCT UNNEST(%s::text[]) AS zjhm
            )
            SELECT
                ids.zjhm AS "身份证号",
                COUNT(*) AS "数量"
            FROM ids
            JOIN "ywdata"."zq_zfba_xyrxx" zzx
              ON zzx."xyrxx_sfzh" = ids.zjhm
            WHERE zzx."ajxx_join_ajxx_ajbh" IS NOT NULL
            GROUP BY ids.zjhm
            """,
            (normalized,),
        ),
        "alarm": _query_count_map(
            """
            SELECT
                jqm."sfzh" AS "身份证号",
                COUNT(*) AS "数量"
            FROM "jcgkzx_monitor"."jszahz_jq_sfzh_map" jqm
            WHERE jqm."sfzh" = ANY(%s::text[])
            GROUP BY jqm."sfzh"
            """,
            (normalized,),
        ),
        "vehicle": _query_count_map(
            """
            WITH ids AS (
                SELECT DISTINCT UNNEST(%s::text[]) AS zjhm
            )
            SELECT
                ids.zjhm AS "身份证号",
                COUNT(*) AS "数量"
            FROM ids
            JOIN "ywdata"."t_qsjdc_jbxx" jdc
              ON jdc."sfzmhm" = ids.zjhm
            GROUP BY ids.zjhm
            """,
            (normalized,),
        ),
        "video": _query_count_map(
            """
            SELECT
                spy."id_number" AS "身份证号",
                COUNT(*) AS "数量"
            FROM "ywdata"."t_spy_ryrlgj_xx" spy
            WHERE spy."libname" = '精神病人'
              AND spy."id_number" = ANY(%s::text[])
            GROUP BY spy."id_number"
            """,
            (normalized,),
        ),
        "clinic": _query_count_map(
            """
            WITH ids AS (
                SELECT DISTINCT UNNEST(%s::text[]) AS zjhm
            )
            SELECT
                ids.zjhm AS "身份证号",
                COUNT(*) AS "数量"
            FROM ids
            JOIN "ywdata"."sh_yf_mz_djxx" mz
              ON mz."zjhm" = ids.zjhm
            GROUP BY ids.zjhm
            """,
            (normalized,),
        ),
        "racing": _query_count_map(
            """
            WITH ids AS (
                SELECT DISTINCT UNNEST(%s::text[]) AS zjhm
            )
            SELECT
                ids.zjhm AS "身份证号",
                COUNT(*) AS "数量"
            FROM ids
            JOIN "ywdata"."b_evt_jjzdbczjajxx" jz
              ON jz."dsrsfzmhm" = ids.zjhm
            GROUP BY ids.zjhm
            """,
            (normalized,),
        ),
    }


def query_case_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            zzx."ajxx_join_ajxx_ajbh" AS "案件编号",
            zzx."ajxx_join_ajxx_ajlx" AS "案件类型",
            zzx."ajxx_join_ajxx_ay" AS "案由",
            zzx."ajxx_join_ajxx_cbdw_bh" AS "办案单位",
            zzx."ajxx_join_ajxx_lasj" AS "立案时间"
        FROM "ywdata"."zq_zfba_xyrxx" zzx
        WHERE zzx."ajxx_join_ajxx_ajbh" IS NOT NULL
          AND zzx."xyrxx_sfzh" = %s
        ORDER BY zzx."ajxx_join_ajxx_lasj" DESC NULLS LAST
        """,
        (zjhm,),
    )


def query_alarm_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            jqm."calltime" AS "报警时间",
            jqm."cmdname" AS "分局",
            jqm."dutydeptname" AS "管辖单位",
            jqm."casecontents" AS "报警内容",
            jqm."newcharasubclass" AS "警情性质",
            jqm."occuraddress" AS "警情地址",
            jqm."replies" AS "处警情况",
            jqm."newcharasubclass" AS "确认警情编码",
            jqm."neworicharasubclass" AS "原始警情编码",
            jqm."cmdid" AS "分局编码",
            jqm."dutydeptno" AS "管辖单位编码",
            jqm."caseno" AS "警情编号"
        FROM "jcgkzx_monitor"."jszahz_jq_sfzh_map" jqm
        WHERE jqm."sfzh" = %s
        ORDER BY jqm."source_sync_ts" DESC, jqm."source_jq_id" DESC
        """,
        (zjhm,),
    )


def query_vehicle_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            jdc."hpzl" AS "号牌种类",
            jdc."jdcxh" AS "机动车序号",
            jdc."hphmqc" AS "号牌名称",
            jdc."zwppmc" AS "中文品牌",
            jdc."clzzqymc" AS "车辆制造企业",
            jdc."cllx" AS "车辆类型",
            jdc."ccdjrq" AS "初次登记时间",
            jdc."zjdjrq" AS "最近定检日期",
            jdc."qzbfqz" AS "强制报废期止"
        FROM "ywdata"."t_qsjdc_jbxx" jdc
        WHERE jdc."sfzmhm" = %s
        ORDER BY jdc."ccdjrq" DESC NULLS LAST
        """,
        (zjhm,),
    )


def query_video_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            spy."name" AS "姓名",
            spy."shot_time" AS "抓拍时间",
            dev."azdz" AS "地点",
            dev."jd" AS "经度",
            dev."wd" AS "纬度"
        FROM "ywdata"."t_spy_ryrlgj_xx" spy
        LEFT JOIN "ywdata"."t_yf_spy_qs_device" dev
          ON spy."device_id" = dev."sbbm"
        WHERE spy."libname" = '精神病人'
          AND spy."id_number" = %s
        ORDER BY
            CASE
                WHEN spy."shot_time" ~ '^\\d{14}$' THEN ywdata.str_to_ts(spy."shot_time")
                ELSE NULL
            END DESC NULLS LAST,
            spy."shot_time" DESC NULLS LAST
        """,
        (zjhm,),
    )


def query_clinic_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            mz."xzz_dzmc" AS "现住址",
            mz."gzdw" AS "工作单位",
            mz."lxr_xm" AS "联系人姓名",
            mz."mzyy_yymc" AS "医院名称",
            mz."mzyy_ksmc" AS "科室名称",
            mz."mzys_ysxm" AS "医生姓名",
            mz."mzxx_mzfy" AS "门诊费用",
            mz."mzxx_jzsj" AS "就诊时间"
        FROM "ywdata"."sh_yf_mz_djxx" mz
        WHERE mz."zjhm" = %s
        ORDER BY mz."mzxx_jzsj" DESC NULLS LAST
        """,
        (zjhm,),
    )


def query_racing_rows(zjhm: str) -> List[Dict[str, Any]]:
    return execute_query(
        """
        SELECT
            jz."wsbh" AS "文书编号",
            jz."wfsj" AS "违法时间",
            jz."wfdd" AS "违法地点",
            jz."hphm" AS "号牌号码",
            jz."wfxw" AS "违法行为",
            jz."shyj" AS "审核意见"
        FROM "ywdata"."b_evt_jjzdbczjajxx" jz
        WHERE jz."dsrsfzmhm" = %s
        ORDER BY jz."wfsj" DESC NULLS LAST
        """,
        (zjhm,),
    )
