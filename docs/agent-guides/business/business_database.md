# Business Database Reference

## Purpose

Use this file when a task requires querying or analyzing business data related to police incidents, case records, administrative penalties, criminal detention, arrests, prosecution records, suspect information, or case-involved person information.

## Database

- The database engine is Kingbase V8.
- Write SQL with Kingbase V8 compatibility in mind.
- Prefer explicit schema-qualified table names.
- Before writing SQL against unfamiliar fields, inspect the table structure when possible.

## SQL Rules

- Do not use empty-string comparisons such as `column <> ''`.
- Use `column IS NOT NULL` when filtering for available values.
- If true empty-string handling is required, confirm the actual Kingbase V8 behavior before adding additional filters.

## Core Tables

| Data domain | Table |
| --- | --- |
| Police incident / call records | `"ywdata"."zq_kshddpt_dsjfx_jq"` |
| Case information | `"ywdata"."zq_zfba_ajxx"` |
| Administrative penalty decision | `"ywdata"."zq_zfba_xzcfjds"` |
| Criminal detention | `"ywdata"."zq_zfba_jlz"` |
| Arrest | `"ywdata"."zq_zfba_dbz"` |
| Prosecution person information | `"ywdata"."zq_zfba_qsryxx"` |
| Suspect information | `"ywdata"."zq_zfba_xyrxx"` |
| Case-involved person information | `"ywdata"."zq_zfba_saryxx"` |

## Important Notes

- `"ywdata"."zq_zfba_saryxx"` may not exist in the current local database clone yet. Use this table name as the expected production table for case-involved person information, and inspect its structure after the local schema is synchronized.
- Do not use `"ywdata"."zq_zfba_wcnr_ajxx"` as the general case-information table. The general case-information table is `"ywdata"."zq_zfba_ajxx"`.
