CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_zq_zfba_xyrxx_sfzh_ajbh_lasj
ON "ywdata"."zq_zfba_xyrxx" ("xyrxx_sfzh", "ajxx_join_ajxx_ajbh", "ajxx_join_ajxx_lasj");

CREATE INDEX IF NOT EXISTS idx_zq_kshddpt_dsjfx_jq_replies_trgm
ON "ywdata"."zq_kshddpt_dsjfx_jq"
USING gin ("replies" gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_t_qsjdc_jbxx_sfzmhm_ccdjrq
ON "ywdata"."t_qsjdc_jbxx" ("sfzmhm", "ccdjrq");

CREATE INDEX IF NOT EXISTS idx_b_evt_jjzdbczjajxx_dsrsfzmhm_wfsj
ON "ywdata"."b_evt_jjzdbczjajxx" ("dsrsfzmhm", "wfsj");

CREATE INDEX IF NOT EXISTS idx_t_spy_ryrlgj_xx_libname_id_number_shot_time
ON "ywdata"."t_spy_ryrlgj_xx" ("libname", "id_number", "shot_time");

CREATE INDEX IF NOT EXISTS idx_t_yf_spy_qs_device_sbbm
ON "ywdata"."t_yf_spy_qs_device" ("sbbm");

CREATE INDEX IF NOT EXISTS idx_sh_yf_mz_djxx_zjhm_mzxx_jzsj
ON "ywdata"."sh_yf_mz_djxx" ("zjhm", "mzxx_jzsj");
