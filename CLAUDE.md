# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a police data analysis and statistics system (警情案件管控中心) built with Flask + PostgreSQL. The system provides modules for:

- **警情案件** (`jingqing_anjian/`) - Case query, statistics, and report generation
- **巡防统计** (`xunfang/`) - Patrol statistics, online rate tracking, quadrant chart generation
- **治综平台数据统计** (`zhizong/`) - Integrated platform data statistics with configurable tasks
- **未成年人** (`weichengnianren/`) - Minors-related data management

## Common Commands

### Running the Application
```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment (copy .env.example to .env and modify)
cp .env.example .env

# Start the development server (runs on port 5003)
python app.py
```

### Database
- Database: PostgreSQL (KingbaseES compatible via `psycopg2`)
- Connection configured via environment variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`
- Default schema: `ywdata`

### Testing
- This system makes requests to internal network addresses (公司内网) that are not accessible from external networks
- Do not attempt to test network-dependent features in non-internal network environments

## Architecture

### Module Structure

Each business module follows a three-layer architecture:

```
<module_name>/
  routes/          - Flask route handlers (蓝图中转层)
  service/         - Business logic (业务逻辑层)
  dao/             - Data access objects (数据访问层)
  templates/       - Jinja2 templates (前端模板)
```

### Global Components (`gonggong/`)

- **`config/database.py`** - Database connection management with `DB_CONFIG` and `get_database_connection()`, `execute_query()`
- **`service/session_manager.py`** - Global session manager for internal API requests (68.253.2.107), with auto-login and retry logic
- **`config/display_fields.py`** - Field display configuration for data tables
- **`service/mapping_service.py`** - Code-to-name mapping services (e.g., region codes)

### Blueprints Registration

Blueprints are registered in `app.py`:
```python
app.register_blueprint(xunfang_bp, url_prefix="/xunfang")
app.register_blueprint(zhizong_bp, url_prefix="/zhizong")
app.register_blueprint(jingqing_anjian_bp, url_prefix="/jingqing_anjian")
app.register_blueprint(weichengnianren_bp, url_prefix="/weichengnianren")
```

### Authentication & Authorization

- User login via `/login` route, credentials stored in `ywdata.jcgkzx_user` table
- Module access controlled by `ywdata.jcgkzx_permission` table (module values: 警情/巡防/治综/后台/未成年人)
- Each blueprint has `@before_request` to check permissions

### Session Manager (`session_manager`)

The global `session_manager` handles authentication to internal APIs:
- Auto-login on first request or session expiry
- 4-hour session validity with 30-minute pre-expiry checks
- Automatic retry on failure with cooldown periods
- Target host: `68.253.2.107`

Usage:
```python
from gonggong.service.session_manager import session_manager
response = session_manager.make_request('GET', url, params=params)
```

## Database Patterns

### SQL Functions vs Direct Queries

The project uses both PostgreSQL functions and direct SQL queries:
- SQL functions (stored procedures) are called via `execute_query()` with `SELECT function_name(...)`
- Direct queries use parameterized queries to prevent SQL injection

### Key Database Tables

- `ywdata.jcgkzx_user` - User credentials
- `ywdata.jcgkzx_permission` - User-module permissions
- `ywdata.sys_dq_pcs` - Police station region codes and names
- `ywdata.task_metadata` - Zhizong module task configurations
- `stdata.*` - Various statistics tables (schema for static data)

### Region Code Mapping

Region codes (first 6 digits of `deptId`):
- `445302` - 云城
- `445303` - 云安
- `445321` - 新兴
- `445322` - 郁南
- `445381` - 罗定

## File Encoding

- All repository files use UTF-8 encoding
- When viewing Chinese content in PowerShell, use: `Get-Content -Encoding UTF8 <file>`

## Export Features

### Excel Exports (openpyxl)
- Online rate statistics (`export_online_rate_for_date_range`)
- Quadrant charts with matplotlib (`export_quadrant_chart_for_date_range`)
- Police force tables (`export_police_force_for_date_range`)

### Word Exports (docxtpl)
- Police case analysis reports (`jingqing_yanpan_service`)
- Template files located in `jingqing_anjian/templates/template.docx`

## Development Guidelines

1. Preserve existing route/interface/template structure when adding features
2. Follow the routes → service → dao layering pattern
3. Use `session_manager` for all requests to 68.253.2.107
4. Use parameterized queries for database operations
5. Handle errors gracefully with try-except blocks
6. Add Chinese comments for complex business logic
