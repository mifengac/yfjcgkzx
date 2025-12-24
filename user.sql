-- 用户表：jcgkzx_user
-- 说明：存储系统登录用户，密码使用Werkzeug哈希加密
-- 数据库：PostgreSQL/Kingbase 兼容
-- Schema：ywdata

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE TABLE IF NOT EXISTS "ywdata"."jcgkzx_user" (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL
);

COMMENT ON TABLE "ywdata"."jcgkzx_user" IS '用户表';
COMMENT ON COLUMN "ywdata"."jcgkzx_user".id IS '用户唯一ID（自增主键）';
COMMENT ON COLUMN "ywdata"."jcgkzx_user".username IS '登录账号';
COMMENT ON COLUMN "ywdata"."jcgkzx_user".password IS '登录密码（Werkzeug哈希）';

