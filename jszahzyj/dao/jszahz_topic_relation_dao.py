from __future__ import annotations

from typing import Dict, List

from gonggong.config.database import execute_query
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
        FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq
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
        FROM "ywdata"."t_spy_ryrlgj_xx" spy
        LEFT JOIN "ywdata"."t_yf_spy_qs_device" dev
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
