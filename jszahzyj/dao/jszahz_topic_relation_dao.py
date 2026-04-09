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


def _build_in_placeholders(values: List[str]) -> str:
    if not values:
        raise ValueError("身份证号列表不能为空")
    return ", ".join(["%s"] * len(values))


def _rows_to_count_map(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        str(row.get("zjhm") or "").strip().upper(): int(row.get("total_count") or 0)
        for row in (rows or [])
        if str(row.get("zjhm") or "").strip()
    }


def query_relation_count_maps(zjhms: Iterable[str]) -> Dict[str, Dict[str, int]]:
    normalized = _normalize_zjhms(zjhms)
    if not normalized:
        return {
            "case": {},
            "alarm": {},
            "vehicle": {},
            "video": {},
            "clinic": {},
        }

    in_placeholders = _build_in_placeholders(normalized)
    in_params = tuple(normalized)
    values_placeholders = ", ".join(["(%s)"] * len(normalized))
    values_params = tuple(normalized)

    case_rows = execute_query(
        f"""
        SELECT
            zzx."xyrxx_sfzh" AS zjhm,
            COUNT(*) AS total_count
        FROM "ywdata"."zq_zfba_xyrxx" zzx
        WHERE zzx."ajxx_join_ajxx_ajbh" IS NOT NULL
          AND zzx."xyrxx_sfzh" IN ({in_placeholders})
        GROUP BY zzx."xyrxx_sfzh"
        """,
        in_params,
    )

    alarm_rows = execute_query(
        f"""
        WITH ids(zjhm) AS (
            VALUES {values_placeholders}
        )
        SELECT
            ids.zjhm AS zjhm,
            COUNT(jq."replies") AS total_count
        FROM ids
        LEFT JOIN "zq_kshddpt_dsjfx_jq" jq
          ON POSITION(ids.zjhm IN COALESCE(jq."replies", '')) > 0
        GROUP BY ids.zjhm
        """,
        values_params,
    )

    vehicle_rows = execute_query(
        f"""
        SELECT
            jdc."sfzmhm" AS zjhm,
            COUNT(*) AS total_count
        FROM "ywdata"."t_qsjdc_jbxx" jdc
        WHERE jdc."sfzmhm" IN ({in_placeholders})
        GROUP BY jdc."sfzmhm"
        """,
        in_params,
    )

    video_rows = execute_query(
        f"""
        SELECT
            spy."id_number" AS zjhm,
            COUNT(*) AS total_count
        FROM "t_spy_ryrlgj_xx" spy
        WHERE spy."libname" = '精神病人'
          AND spy."id_number" IN ({in_placeholders})
        GROUP BY spy."id_number"
        """,
        in_params,
    )

    clinic_rows = execute_query(
        f"""
        SELECT
            mz."zjhm" AS zjhm,
            COUNT(*) AS total_count
        FROM "ywdata"."sh_yf_mz_djxx" mz
        WHERE mz."zjhm" IN ({in_placeholders})
        GROUP BY mz."zjhm"
        """,
        in_params,
    )

    return {
        "case": _rows_to_count_map(case_rows),
        "alarm": _rows_to_count_map(alarm_rows),
        "vehicle": _rows_to_count_map(vehicle_rows),
        "video": _rows_to_count_map(video_rows),
        "clinic": _rows_to_count_map(clinic_rows),
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
            jq."calltime" AS "报警时间",
            jq."cmdname" AS "分局",
            jq."dutydeptname" AS "管辖单位",
            jq."casecontents" AS "报警内容",
            jq."newcharasubclassname" AS "警情性质"
        FROM "zq_kshddpt_dsjfx_jq" jq
        WHERE POSITION(%s IN COALESCE(jq."replies", '')) > 0
        ORDER BY jq."calltime" DESC NULLS LAST
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
        FROM "t_spy_ryrlgj_xx" spy
        LEFT JOIN "t_yf_spy_qs_device" dev
          ON spy."device_id" = dev."sbbm"
        WHERE spy."libname" = '精神病人'
          AND spy."id_number" = %s
        ORDER BY spy."shot_time" DESC NULLS LAST
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
