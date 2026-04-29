# AGENTS.md

## Purpose

This file gives coding agents the minimum project context needed to make safe, targeted changes in this repository.

Prefer small, explicit edits. Preserve existing behavior unless the task clearly requires a behavior change.

## Project Summary

This repository is a Flask-based internal police business platform. It serves multiple business modules from one application, with login and permission-gated access, internal data integrations, reporting/export flows, and several specialized analysis pages.

Primary stack:

- Backend: Flask, waitress, psycopg2, requests
- Data and reporting: pandas, openpyxl, xlrd/xlwt/xlutils, python-docx, docxtpl, reportlab, matplotlib, Pillow
- Optional AI and inference: local OpenAI-compatible llama endpoint, torch, transformers
- Runtime integrations: Kingbase or PostgreSQL-compatible database, upstream internal systems on fixed intranet IPs, optional Oracle-backed SMS writing

## Repository Map

- `app.py`: Flask entrypoint, login/logout, main menu, health/debug endpoints, and blueprint registration
- `gonggong/config/`: shared configuration such as database, upstream host config, access control, and display field mappings
- `gonggong/service/`: shared integrations and helpers such as upstream session handling, mapping logic, and HTTP clients
- `gonggong/dao/`: shared data access helpers
- `gonggong/utils/`: reusable utilities such as request helpers and error handling
- `<module>/routes/`: Flask blueprints and HTTP handlers for each business module
- `<module>/service/`: business orchestration, aggregation, export, and domain logic
- `<module>/dao/`: SQL execution and low-level data shaping
- `<module>/templates/` and `<module>/static/`: module-local frontend templates and browser assets
- `templates/` and `static/`: shared application templates and static assets
- `tests/`: unittest-style tests that also run under `pytest`
- `SQL/`: SQL scripts for views, materialized views, and schema updates
- `scripts/`: one-off maintenance or generation scripts
- `weichengnianren-djdo/wcnr_djdo/`: special submodule loaded via `sys.path` because the parent directory name contains a hyphen
- `docs/agent-guides/`: domain reference guides that extend these AGENTS instructions by topic

## Functional Modules

Key modules in active use include:

- Incident and case pages: `jingqing_anjian`, `jingqing_fenxi`, `jingqing_anjian_fenxi`
- Patrol and comprehensive statistics: `xunfang`, `zhizong`, `hqzcsj`
- Special-population and thematic modules: `weichengnianren`, `weichengnianren-djdo`, `gzrzdd`, `jszahzyj`, `mdjfxsyj`
- School scoring and ranking: `xxffmk`
- Data extraction and AI-assisted field extraction: `shuju_tiqu`
- Backend administration: `houtai` user import, permission import, and SMS management

## Domain Reference Guides

- `docs/agent-guides/business/business_database.md`: read before queries or analysis involving police incidents, case records, administrative penalties, criminal detention, arrests, prosecution records, suspect information, or case-involved person information.
- `docs/agent-guides/integrations/dsjjqfx.md`: read before changes involving `http://68.253.2.111/dsjfx`, `/plan/treeViewData`, `/nature/treeNewViewData`, `/case/list`, or `/srr/list`.
- `docs/agent-guides/region/region_grouping.md`: read before coding or SQL tasks involving region grouping, county/district grouping, police-station grouping, organization codes, or area-code/name mapping.

## Local Development

Recommended local workflow:

- Copy `.env.example` to `.env` and fill in the required internal credentials and endpoints
- Install dependencies: `pip install -r requirements.txt`
- Run the app directly: `python app.py`
- Alternative debug run: `flask --app app run --host 0.0.0.0 --port 5003 --debug`
- Run tests: `pytest`
- Build the container image: `docker build -t yfjcgkzx .`

Use Python 3.12 locally when possible to match the Dockerfile. Some optional dependencies are environment-specific, especially `pywin32`, `oracledb`, and local model/runtime dependencies.

## Environment and Runtime Constraints

- `gonggong.config.database` loads `.env` and database settings at import time. Set `DB_*` environment variables before importing modules that touch the database.
- Database client encoding matters. The project explicitly sets `PGCLIENTENCODING=GB18030` and falls back to `GBK` for some connection-error scenarios. Do not casually remove or rewrite that behavior.
- `app.py` appends both the project root and `weichengnianren-djdo/` to `sys.path` so the `wcnr_djdo` package can be imported. Preserve this unless you are deliberately changing the import strategy.
- The app generates a new Flask `SECRET_KEY` on startup. Be careful when changing session handling or login flow because restart behavior affects active sessions.
- Many modules rely on `session["username"]` plus rows in `ywdata.jcgkzx_permission` for access control. Preserve the current login and permission pattern when editing route code.
- Shared upstream session handling lives in `gonggong.service.session_manager`. It is designed to log in lazily and includes retry and cooldown behavior. Avoid turning that into eager import-time network traffic.
- Several modules depend on internal upstream systems such as `68.253.2.111`, `68.253.2.107`, and `68.29.177.247`, plus a local OpenAI-compatible llama endpoint. Do not assume public internet access or replace internal calls with internet services.
- User-facing UI copy and many business fields are Chinese. Some files or outputs may look garbled in a non-UTF-8 terminal. Verify actual file contents carefully before mass editing Chinese strings.

## Testing Guidance

- Tests follow `unittest` structure but run under `pytest`.
- Prefer focused test runs such as `pytest tests/test_xxffmk_routes.py` or the closest module-specific test file.
- For route tests, mirror the existing pattern: build a minimal Flask app, register the target blueprint, seed `session["username"]`, and patch `get_database_connection` plus service calls.
- For service tests around upstream access, patch `requests.Session`, client wrappers, or DAO calls instead of hitting real internal systems.
- Do not require live Kingbase, upstream incident systems, prison data services, local LLM services, Oracle, or SMS services for automated tests.
- After every code change, perform a functional verification of the changed logic. Use the closest realistic validation path available, such as a focused automated test, a dry run, a targeted API call, or a UI interaction check.

## Change Guidelines

- Keep Flask route handlers and blueprints thin. Put orchestration, aggregation, export, and transformation logic in `service` modules.
- Keep SQL execution, pagination helpers, and low-level row shaping in `dao` modules.
- Reuse shared code under `gonggong/` for database access, upstream clients, mapping, session handling, and utilities instead of re-implementing the same logic in business modules.
- When adding or expanding a business module, follow the existing `routes/service/dao/templates/static` layout rather than growing `app.py`.
- Preserve current login, permission, endpoint, and template contracts unless the task explicitly changes them.
- Keep upstream raw field normalization and compatibility handling in service or adapter code, not scattered across templates and route files.
- Frontend edits should stay within plain HTML, CSS, and browser-side JavaScript. There is no Node build step or JS bundler in this repository.
- Prefer decoupled code with clear responsibilities. Split routing, orchestration, data access, rendering helpers, and reusable utility logic so that one file does not accumulate multiple unrelated responsibilities.
- Keep code files small and maintainable. As a hard rule, any single `js`, `html`, or `py` file should stay within 400 lines whenever feasible; if a change would push a file beyond that limit, split the code into smaller modules, helpers, partials, or service units first.
- If you change login flow, permission lookup, upstream session handling, SQL contracts, export logic, LLM extraction logic, or Oracle/SMS writeback behavior, update tests and call out the behavior change clearly.

## Done When

A task is done when all of the following are true:

- The requested behavior is implemented and the affected page, API, or export flow works end to end.
- Existing login, session, permission gating, and blueprint registration behavior is not broken unintentionally.
- No database schema assumptions, upstream integration contracts, report output, or business-module routing behavior are broken unintentionally.
- Tests were updated or added when behavior changed, and the relevant test command was run when feasible.
- The modified code path was functionally validated after the edit, not only reviewed statically.
- If verification could not be completed because of environment constraints such as unavailable internal systems, Oracle, or local model services, that limitation is stated explicitly in the final handoff.
- Documentation or configuration examples were updated when the change altered required environment variables, operator-facing behavior, or integration assumptions.

## Safety Notes

- Never hardcode or log secrets. Treat `.env` and all internal host credentials as environment-specific configuration.
- Be careful with import-time behavior. Database config loading, shared singletons, and blueprint side-effect imports can break the app before any request is served.
- Preserve schema and table names under `ywdata` unless the task explicitly changes database contracts.
- Be careful with text encoding, CSV exports, Excel imports, and Chinese field names. Data format regressions are easy to introduce and hard to notice without validation.
- Prefer incremental changes over broad refactors unless the task explicitly asks for a structural rewrite.

## Good First Reads

Before making non-trivial changes, read these files first:

- `app.py`
- `.env.example`
- `gonggong/config/database.py`
- `gonggong/service/session_manager.py` if the code path involves upstream login or authenticated requests
- the relevant module under `routes/`, `service/`, and `dao/`
- the closest existing test under `tests/`
- `docs/agent-guides/integrations/dsjjqfx.md` first if the code path involves `http://68.253.2.111` or `/dsjfx/`
