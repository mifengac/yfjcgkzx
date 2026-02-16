-- b_dxpt_mdjfyj 唯一约束迁移脚本
-- 规则：按 (sspcsdm, xm) 唯一，供后台短信发送管理覆盖写入使用。

CREATE SCHEMA IF NOT EXISTS "ywdata";

-- 1) 可选：检查重复数据
-- SELECT sspcsdm, xm, COUNT(*)
-- FROM "ywdata"."b_dxpt_mdjfyj"
-- GROUP BY sspcsdm, xm
-- HAVING COUNT(*) > 1;

-- 2) 如存在重复，先保留 lrsj 最新一条并删除其余记录
WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY sspcsdm, xm
            ORDER BY lrsj DESC NULLS LAST, xh DESC NULLS LAST
        ) AS rn
    FROM "ywdata"."b_dxpt_mdjfyj"
)
DELETE FROM "ywdata"."b_dxpt_mdjfyj" t
USING ranked r
WHERE t.ctid = r.ctid
  AND r.rn > 1;

-- 3) 创建唯一约束（已存在则忽略）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_b_dxpt_mdjfyj_sspcsdm_xm'
    ) THEN
        ALTER TABLE "ywdata"."b_dxpt_mdjfyj"
        ADD CONSTRAINT uq_b_dxpt_mdjfyj_sspcsdm_xm UNIQUE (sspcsdm, xm);
    END IF;
END$$;
