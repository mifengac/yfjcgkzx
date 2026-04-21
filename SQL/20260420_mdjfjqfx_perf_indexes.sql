-- 矛盾纠纷警情统计性能建议
-- 用途：页面会按警情编号批量匹配案件表，判断警情是否已转案。
-- 注意：本文件仅为 DBA 手工执行建议，应用代码不会自动执行 DDL。

CREATE INDEX IF NOT EXISTS idx_zq_zfba_ajxx_ajxx_jqbh
ON ywdata.zq_zfba_ajxx (ajxx_jqbh);

ANALYZE ywdata.zq_zfba_ajxx;
ANALYZE stdata.b_dic_zzjgdm;
