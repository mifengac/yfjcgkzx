# Region Grouping Reference

## Purpose

Read this document before writing code or SQL that involves region grouping, county/district grouping, police-station grouping, organization-code parsing, or region-code/name mapping.

The goal is to keep region logic consistent across projects and avoid repeatedly re-deriving the same rules.

## Region Levels

Business statistics usually distinguish two grouping levels:

- County/district region
- Police station

All organization codes are 12-character values.

## Code Rules

- The first 6 characters represent the county/district region.
- The first 8 characters represent the police station.
- County/district codes are usually stored in the form `XXXXXX000000`.
- Police-station codes are usually stored in the form `XXXXXXXX0000`.

When source data contains a full 12-character organization code:

- Use `LEFT(code, 6)` for county/district grouping.
- Use `LEFT(code, 8)` for police-station grouping.

When joining to dictionary codes, normalize the prefix back to the 12-character dictionary format when needed:

- County/district dictionary code: `LEFT(code, 6) || '000000'`
- Police-station dictionary code: `LEFT(code, 8) || '0000'`

## Dictionary Table

The citywide code/name mapping table is:

```sql
stdata.b_dic_zzjgdm
```

Important fields:

- `ssfjdm`: county/district code, usually formatted as `XXXXXX000000`.
- `ssfj`: county/district name corresponding to `ssfjdm`.
- `sspcsdm`: police-station code, usually formatted as `XXXXXXXX0000`.
- `sspcs`: police-station name corresponding to `sspcsdm`.

## Recommended SQL Pattern

County/district grouping:

```sql
SELECT
    d.ssfjdm,
    d.ssfj,
    COUNT(*) AS total
FROM source_table s
LEFT JOIN stdata.b_dic_zzjgdm d
  ON d.ssfjdm = LEFT(s.org_code, 6) || '000000'
GROUP BY d.ssfjdm, d.ssfj;
```

Police-station grouping:

```sql
SELECT
    d.sspcsdm,
    d.sspcs,
    COUNT(*) AS total
FROM source_table s
LEFT JOIN stdata.b_dic_zzjgdm d
  ON d.sspcsdm = LEFT(s.org_code, 8) || '0000'
GROUP BY d.sspcsdm, d.sspcs;
```

## Notes

- Prefer using `stdata.b_dic_zzjgdm` for official region and police-station names instead of hardcoding mappings in code.
- If a module already has a local region-name helper, check whether it should be replaced with or validated against `stdata.b_dic_zzjgdm`.
- Be explicit about whether a statistic is grouped by county/district or by police station; do not mix the 6-character and 8-character levels in one aggregation unless the output clearly labels both.
