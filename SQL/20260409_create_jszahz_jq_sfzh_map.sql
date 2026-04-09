CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

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
