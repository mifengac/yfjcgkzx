-- 权限表：jcgkzx_permission
-- 说明：按用户名分配可访问模块；module 取值：巡防/治综/警情/后台
-- 数据库：PostgreSQL/Kingbase 兼容
-- Schema：ywdata

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE TABLE IF NOT EXISTS "ywdata"."jcgkzx_permission" (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    module VARCHAR(50) NOT NULL,
    UNIQUE (username, module)
);

COMMENT ON TABLE "ywdata"."jcgkzx_permission" IS '权限表';
COMMENT ON COLUMN "ywdata"."jcgkzx_permission".id IS '权限记录ID（自增主键）';
COMMENT ON COLUMN "ywdata"."jcgkzx_permission".username IS '登录账号（与用户表对应）';
COMMENT ON COLUMN "ywdata"."jcgkzx_permission".module IS '可访问的模块名（巡防/治综/警情/后台）';

-- 可选：外键关联用户表（如目标库兼容则开启）
-- ALTER TABLE "ywdata"."jcgkzx_permission"
--   ADD CONSTRAINT fk_permission_user
--   FOREIGN KEY (username) REFERENCES "ywdata"."jcgkzx_user"(username)
--   ON DELETE CASCADE;

-- 20260228 迁移：将 module='矛盾纠纷线索移交' 统一改名为 '矛盾纠纷'
-- 注意：UNIQUE(username, module) 约束，先删除冲突行再更新
DELETE FROM "ywdata"."jcgkzx_permission"
WHERE module = '矛盾纠纷'
  AND username IN (
      SELECT username FROM "ywdata"."jcgkzx_permission" WHERE module = '矛盾纠纷线索移交'
  );

UPDATE "ywdata"."jcgkzx_permission"
SET module = '矛盾纠纷'
WHERE module = '矛盾纠纷线索移交';

