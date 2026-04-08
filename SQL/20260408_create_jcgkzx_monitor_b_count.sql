CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."b_count" (
    "bm" VARCHAR NULL,
    "bzw" VARCHAR NULL,
    "zs" VARCHAR NULL,
    "gxsj" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE "jcgkzx_monitor"."custom_case_monitor_scheme" IS U&'\81EA\5B9A\4E49\8B66\60C5\76D1\6D4B\65B9\6848\8868';
COMMENT ON TABLE "jcgkzx_monitor"."custom_case_monitor_rule" IS U&'\81EA\5B9A\4E49\8B66\60C5\76D1\6D4B\89C4\5219\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_batch" IS U&'\7CBE\795E\969C\788D\4E13\9898\6279\6B21\5BFC\5165\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_person_type" IS U&'\7CBE\795E\969C\788D\4E13\9898\4EBA\5458\6807\7B7E\660E\7EC6\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_snapshot" IS U&'\7CBE\795E\969C\788D\4E13\9898\5FEB\7167\7ED3\679C\8868';
COMMENT ON TABLE "jcgkzx_monitor"."b_count" IS U&'\8868\884C\6570\7EDF\8BA1\8868';

CREATE OR REPLACE FUNCTION "jcgkzx_monitor"."table_tj"(schema_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    rec RECORD;
    total_count BIGINT;
BEGIN
    TRUNCATE TABLE "jcgkzx_monitor"."b_count";

    FOR rec IN
        SELECT
            t.table_name AS table_name,
            obj_description((schema_name || '.' || t.table_name)::regclass, 'pg_class') AS table_comment
        FROM information_schema.tables t
        WHERE t.table_schema = schema_name
          AND t.table_type = 'BASE TABLE'
          AND t.table_name <> 'b_count'
        ORDER BY t.table_name
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, rec.table_name) INTO total_count;

        INSERT INTO "jcgkzx_monitor"."b_count" ("bm", "bzw", "zs")
        VALUES (
            rec.table_name,
            COALESCE(rec.table_comment, ''),
            total_count::VARCHAR
        );
    END LOOP;

    INSERT INTO "jcgkzx_monitor"."b_count" ("bm", "bzw", "zs")
    SELECT
        'zzzz_hj',
        U&'\5408\8BA1',
        COALESCE(SUM(COALESCE(NULLIF("zs", ''), '0')::BIGINT), 0)::VARCHAR
    FROM "jcgkzx_monitor"."b_count";
END;
$function$;

SELECT "jcgkzx_monitor"."table_tj"('jcgkzx_monitor');
