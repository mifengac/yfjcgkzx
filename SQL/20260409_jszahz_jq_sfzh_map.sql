-- =====================================================================
-- 精神患者主题库警情身份证号抽取中间表：建表 + 同步存储过程
-- 合并自：create_jszahz_jq_sfzh_map, sync_jszahz_jq_sfzh_map
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

-- ■ 通用同步水位表 ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."sync_watermark" (
    sync_name VARCHAR(128) PRIMARY KEY,
    last_source_sync_ts TIMESTAMP NULL,
    last_source_id INTEGER NULL,
    last_run_started_at TIMESTAMP NULL,
    last_run_finished_at TIMESTAMP NULL,
    last_success_at TIMESTAMP NULL,
    last_status VARCHAR(20) NOT NULL DEFAULT 'idle',
    last_error_message TEXT NULL,
    processed_source_rows BIGINT NOT NULL DEFAULT 0,
    inserted_mapping_rows BIGINT NOT NULL DEFAULT 0
);

-- ■ 警情身份证号抽取中间表 -------------------------------------------------

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_jq_sfzh_map" (
    id BIGSERIAL PRIMARY KEY,
    source_jq_id INTEGER NOT NULL,
    caseno VARCHAR(128) NULL,
    sfzh VARCHAR(18) NOT NULL,
    calltime VARCHAR(64) NULL,
    occuraddress TEXT NULL,
    replies TEXT NULL,
    casecontents TEXT NULL,
    newcharasubclass VARCHAR(128) NULL,
    neworicharasubclass VARCHAR(128) NULL,
    cmdid VARCHAR(64) NULL,
    cmdname VARCHAR(128) NULL,
    dutydeptno VARCHAR(64) NULL,
    dutydeptname VARCHAR(128) NULL,
    source_created_at TIMESTAMP NULL,
    source_updated_at TIMESTAMP NULL,
    source_sync_ts TIMESTAMP NOT NULL,
    extracted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_jszahz_jq_sfzh_map UNIQUE (source_jq_id, sfzh)
);

CREATE INDEX IF NOT EXISTS idx_jszahz_jq_sfzh_map_sfzh_sync_ts
    ON "jcgkzx_monitor"."jszahz_jq_sfzh_map" (sfzh, source_sync_ts DESC, source_jq_id DESC);

CREATE INDEX IF NOT EXISTS idx_jszahz_jq_sfzh_map_source_jq_id
    ON "jcgkzx_monitor"."jszahz_jq_sfzh_map" (source_jq_id);

CREATE INDEX IF NOT EXISTS idx_jszahz_jq_sfzh_map_source_sync_ts_id
    ON "jcgkzx_monitor"."jszahz_jq_sfzh_map" (source_sync_ts, source_jq_id);

INSERT INTO "jcgkzx_monitor"."sync_watermark" (
    sync_name,
    last_status,
    processed_source_rows,
    inserted_mapping_rows
)
VALUES (
    'jszahz_jq_sfzh_map',
    'idle',
    0,
    0
)
ON CONFLICT (sync_name) DO NOTHING;

COMMENT ON TABLE "jcgkzx_monitor"."sync_watermark" IS U&'\901A\7528\6570\636E\540C\6B65\6C34\4F4D\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_jq_sfzh_map" IS U&'\7CBE\795E\60A3\8005\4E3B\9898\5E93\8B66\60C5\8EAB\4EFD\8BC1\53F7\62BD\53D6\4E2D\95F4\8868';

-- ■ 源表辅助索引 -----------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_zq_kshddpt_dsjfx_jq_sync_ts_id
ON "ywdata"."zq_kshddpt_dsjfx_jq" ((COALESCE("updated_at", "created_at")), "id");

-- ■ 增量同步存储过程 -------------------------------------------------------

CREATE OR REPLACE PROCEDURE "jcgkzx_monitor"."sync_jszahz_jq_sfzh_map"(p_force_full BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sync_name CONSTANT VARCHAR(128) := 'jszahz_jq_sfzh_map';
    v_last_source_sync_ts TIMESTAMP;
    v_last_source_id INTEGER;
    v_processed_source_rows BIGINT := 0;
    v_inserted_mapping_rows BIGINT := 0;
    v_new_source_sync_ts TIMESTAMP;
    v_new_source_id INTEGER;
BEGIN
    INSERT INTO "jcgkzx_monitor"."sync_watermark" (
        sync_name,
        last_status,
        processed_source_rows,
        inserted_mapping_rows
    )
    VALUES (
        v_sync_name,
        'idle',
        0,
        0
    )
    ON CONFLICT (sync_name) DO NOTHING;

    SELECT
        last_source_sync_ts,
        last_source_id
    INTO
        v_last_source_sync_ts,
        v_last_source_id
    FROM "jcgkzx_monitor"."sync_watermark"
    WHERE sync_name = v_sync_name
    FOR UPDATE;

    UPDATE "jcgkzx_monitor"."sync_watermark"
    SET
        last_status = 'running',
        last_run_started_at = CURRENT_TIMESTAMP,
        last_run_finished_at = NULL,
        last_error_message = NULL
    WHERE sync_name = v_sync_name;

    IF p_force_full THEN
        TRUNCATE TABLE "jcgkzx_monitor"."jszahz_jq_sfzh_map";
    END IF;

    CREATE TEMP TABLE tmp_jszahz_jq_source
    ON COMMIT DROP
    AS
    SELECT
        src."id" AS source_jq_id,
        src."caseno" AS caseno,
        src."calltime" AS calltime,
        src."occuraddress" AS occuraddress,
        src."replies" AS replies,
        src."casecontents" AS casecontents,
        src."newcharasubclass" AS newcharasubclass,
        src."neworicharasubclass" AS neworicharasubclass,
        src."cmdid" AS cmdid,
        src."cmdname" AS cmdname,
        src."dutydeptno" AS dutydeptno,
        src."dutydeptname" AS dutydeptname,
        src."created_at" AS source_created_at,
        src."updated_at" AS source_updated_at,
        COALESCE(src."updated_at", src."created_at") AS source_sync_ts
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" src
    WHERE
        p_force_full
        OR v_last_source_sync_ts IS NULL
        OR COALESCE(src."updated_at", src."created_at") > v_last_source_sync_ts
        OR (
            COALESCE(src."updated_at", src."created_at") = v_last_source_sync_ts
            AND src."id" > COALESCE(v_last_source_id, 0)
        );

    SELECT COUNT(*) INTO v_processed_source_rows
    FROM tmp_jszahz_jq_source;

    IF v_processed_source_rows > 0 THEN
        DELETE FROM "jcgkzx_monitor"."jszahz_jq_sfzh_map"
        WHERE source_jq_id IN (
            SELECT source_jq_id
            FROM tmp_jszahz_jq_source
        );

        CREATE TEMP TABLE tmp_jszahz_jq_extract
        ON COMMIT DROP
        AS
        SELECT DISTINCT
            src.source_jq_id,
            src.caseno,
            UPPER(match_arr[2]) AS sfzh,
            src.calltime,
            src.occuraddress,
            src.replies,
            src.casecontents,
            src.newcharasubclass,
            src.neworicharasubclass,
            src.cmdid,
            src.cmdname,
            src.dutydeptno,
            src.dutydeptname,
            src.source_created_at,
            src.source_updated_at,
            src.source_sync_ts
        FROM tmp_jszahz_jq_source src
        CROSS JOIN LATERAL regexp_matches(
            COALESCE(src.replies, ''),
            '(^|[^0-9])([0-9]{17}[0-9Xx])([^0-9]|$)',
            'g'
        ) AS match_arr;

        INSERT INTO "jcgkzx_monitor"."jszahz_jq_sfzh_map" (
            source_jq_id,
            caseno,
            sfzh,
            calltime,
            occuraddress,
            replies,
            casecontents,
            newcharasubclass,
            neworicharasubclass,
            cmdid,
            cmdname,
            dutydeptno,
            dutydeptname,
            source_created_at,
            source_updated_at,
            source_sync_ts,
            extracted_at
        )
        SELECT
            source_jq_id,
            caseno,
            sfzh,
            calltime,
            occuraddress,
            replies,
            casecontents,
            newcharasubclass,
            neworicharasubclass,
            cmdid,
            cmdname,
            dutydeptno,
            dutydeptname,
            source_created_at,
            source_updated_at,
            source_sync_ts,
            CURRENT_TIMESTAMP
        FROM tmp_jszahz_jq_extract;

        SELECT COUNT(*) INTO v_inserted_mapping_rows
        FROM tmp_jszahz_jq_extract;

        SELECT
            source_sync_ts,
            source_jq_id
        INTO
            v_new_source_sync_ts,
            v_new_source_id
        FROM tmp_jszahz_jq_source
        ORDER BY source_sync_ts DESC, source_jq_id DESC
        LIMIT 1;
    ELSIF p_force_full THEN
        v_inserted_mapping_rows := 0;
        v_new_source_sync_ts := NULL;
        v_new_source_id := NULL;
    ELSE
        v_inserted_mapping_rows := 0;
        v_new_source_sync_ts := v_last_source_sync_ts;
        v_new_source_id := v_last_source_id;
    END IF;

    UPDATE "jcgkzx_monitor"."sync_watermark"
    SET
        last_source_sync_ts = v_new_source_sync_ts,
        last_source_id = v_new_source_id,
        last_run_finished_at = CURRENT_TIMESTAMP,
        last_success_at = CURRENT_TIMESTAMP,
        last_status = 'success',
        last_error_message = NULL,
        processed_source_rows = v_processed_source_rows,
        inserted_mapping_rows = v_inserted_mapping_rows
    WHERE sync_name = v_sync_name;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE "jcgkzx_monitor"."sync_watermark"
        SET
            last_run_finished_at = CURRENT_TIMESTAMP,
            last_status = 'failed',
            last_error_message = LEFT(SQLERRM, 2000)
        WHERE sync_name = 'jszahz_jq_sfzh_map';
        RAISE;
END;
$$;

-- 首次全量初始化（首次建表或需要重建时手工执行）
-- CALL "jcgkzx_monitor"."sync_jszahz_jq_sfzh_map"(TRUE);

-- 日常增量同步（可手工执行）
-- CALL "jcgkzx_monitor"."sync_jszahz_jq_sfzh_map"(FALSE);

-- 自动同步建议：
-- 由 DBA 在数据库定时任务中每 5 分钟执行一次：
-- CALL "jcgkzx_monitor"."sync_jszahz_jq_sfzh_map"(FALSE);
