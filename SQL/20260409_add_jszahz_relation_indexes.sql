CREATE INDEX IF NOT EXISTS idx_zq_zfba_xyrxx_sfzh_ajbh_lasj
ON "ywdata"."zq_zfba_xyrxx" ("xyrxx_sfzh", "ajxx_join_ajxx_ajbh", "ajxx_join_ajxx_lasj");

-- 当前本地 KingbaseES V008R006C009B0014 未提供 pg_trgm / gin_trgm_ops，
-- 因此 replies 的包含匹配查询暂时无法直接创建 trigram GIN 索引。
-- 若后续库侧提供该扩展，才可恢复原方案；否则建议拆分标准化明细表来承接身份证号检索。

CREATE INDEX IF NOT EXISTS idx_t_qsjdc_jbxx_sfzmhm_ccdjrq
ON "ywdata"."t_qsjdc_jbxx" ("sfzmhm", "ccdjrq");

CREATE INDEX IF NOT EXISTS idx_b_evt_jjzdbczjajxx_dsrsfzmhm_wfsj
ON "ywdata"."b_evt_jjzdbczjajxx" ("dsrsfzmhm", "wfsj");

CREATE INDEX IF NOT EXISTS idx_t_spy_ryrlgj_xx_id_number_libname_shot_time_ts
ON "ywdata"."t_spy_ryrlgj_xx" (
    "id_number",
    "libname",
    (CASE
        WHEN "shot_time" ~ '^\d{14}$' THEN ywdata.str_to_ts("shot_time")
        ELSE NULL
    END)
);

CREATE INDEX IF NOT EXISTS idx_t_yf_spy_qs_device_sbbm
ON "ywdata"."t_yf_spy_qs_device" ("sbbm");

CREATE INDEX IF NOT EXISTS idx_sh_yf_mz_djxx_zjhm_mzxx_jzsj
ON "ywdata"."sh_yf_mz_djxx" ("zjhm", "mzxx_jzsj");
