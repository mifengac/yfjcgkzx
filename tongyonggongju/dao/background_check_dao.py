from __future__ import annotations

from typing import Any, Iterable, List

from gonggong.config.database import execute_query


def _normalize_ids(id_numbers: Iterable[str]) -> List[str]:
    seen = set()
    normalized: List[str] = []
    for value in id_numbers or []:
        text = str(value or "").strip().upper()
        if text and text not in seen:
            seen.add(text)
            normalized.append(text)
    return normalized


def query_prior_case_rows(id_numbers: Iterable[str]) -> List[dict[str, Any]]:
    ids = _normalize_ids(id_numbers)
    if not ids:
        return []

    sql = """
    SELECT DISTINCT
        UPPER(BTRIM(zzx."xyrxx_sfzh")) AS "身份证号",
        zzx."ajxx_join_ajxx_ajbh" AS "案件编号",
        zzx."ajxx_join_ajxx_ajlx" AS "案件类型",
        zzx."ajxx_join_ajxx_ajmc" AS "案件名称",
        zzx."ajxx_join_ajxx_lasj" AS "立案时间",
        zzx."ajxx_join_ajxx_cbdw_bh" AS "办案单位"
    FROM "ywdata"."zq_zfba_xyrxx" zzx
    WHERE zzx."xyrxx_sfzh" IS NOT NULL
      AND UPPER(BTRIM(zzx."xyrxx_sfzh")) = ANY(%s::text[])
      AND zzx."ajxx_join_ajxx_ajbh" IS NOT NULL
    ORDER BY UPPER(BTRIM(zzx."xyrxx_sfzh")),
             zzx."ajxx_join_ajxx_lasj" DESC NULLS LAST,
             zzx."ajxx_join_ajxx_ajbh" DESC NULLS LAST
    """
    return execute_query(sql, (ids,))


def query_dispute_rows(id_numbers: Iterable[str]) -> List[dict[str, Any]]:
    ids = _normalize_ids(id_numbers)
    if not ids:
        return []

    sql = """
    SELECT
        UPPER(BTRIM(p."zjhm")) AS "身份证号",
        p."xm" AS "姓名",
        CASE COALESCE(BTRIM(p."sflg"::text), '')
            WHEN '1' THEN '在管'
            WHEN '0' THEN '撤管'
            ELSE COALESCE(BTRIM(p."sflg"::text), '')
        END AS "管理状态",
        p."lgsj" AS "列管时间",
        p."lgdw" AS "列管单位"
    FROM "stdata"."b_per_mdjffxrygl" p
    WHERE COALESCE(BTRIM(p."deleteflag"::text), '0') = '0'
      AND p."zjhm" IS NOT NULL
      AND UPPER(BTRIM(p."zjhm")) = ANY(%s::text[])
    ORDER BY UPPER(BTRIM(p."zjhm")),
             CASE COALESCE(BTRIM(p."sflg"::text), '') WHEN '1' THEN 0 WHEN '0' THEN 1 ELSE 2 END,
             p."lgsj" DESC NULLS LAST
    """
    return execute_query(sql, (ids,))


def query_mental_health_rows(id_numbers: Iterable[str]) -> List[dict[str, Any]]:
    ids = _normalize_ids(id_numbers)
    if not ids:
        return []

    sql = """
    SELECT
        UPPER(BTRIM(p."zjhm")) AS "身份证号",
        p."xm" AS "姓名",
        CASE COALESCE(BTRIM(p."sflg"::text), '')
            WHEN '1' THEN '在管'
            WHEN '0' THEN '撤管'
            ELSE COALESCE(BTRIM(p."sflg"::text), '')
        END AS "管理状态",
        CASE COALESCE(p."fxdj"::text, '')
            WHEN '00' THEN '0级患者'
            WHEN '01' THEN '1级患者'
            WHEN '02' THEN '2级患者'
            WHEN '03' THEN '3级患者'
            WHEN '04' THEN '4级患者'
            WHEN '05' THEN '5级患者'
            ELSE COALESCE(p."fxdj"::text, '')
        END AS "风险等级",
        p."hjdz" AS "户籍地址",
        p."lgsj" AS "列管时间",
        p."lgdw" AS "列管单位"
    FROM "stdata"."b_per_jszahzryxxwh" p
    WHERE COALESCE(BTRIM(p."deleteflag"::text), '0') = '0'
      AND p."zjhm" IS NOT NULL
      AND UPPER(BTRIM(p."zjhm")) = ANY(%s::text[])
    ORDER BY UPPER(BTRIM(p."zjhm")),
             CASE COALESCE(BTRIM(p."sflg"::text), '') WHEN '1' THEN 0 WHEN '0' THEN 1 ELSE 2 END,
             p."lgsj" DESC NULLS LAST
    """
    return execute_query(sql, (ids,))
