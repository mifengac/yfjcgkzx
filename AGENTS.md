# 项目协作说明（给 Codex/智能体）

## 项目概览
- 技术栈：Python + Flask（入口：`app.py`），PostgreSQL（`psycopg2`），前端以 Jinja2 模板为主。
- 主要模块（蓝图注册见 `app.py`）：
  - `jingqing_anjian/`：警情案件
  - `xunfang/`：巡防统计
  - `zhizong/`：治综平台数据统计
  - `weichengnianren/`：未成年人相关
  - `houtai/`：后台批量导入（可选，导入失败会被忽略）

## 本地运行
1. 安装依赖：`pip install -r requirements.txt`
2. 配置环境变量：
   - 复制 ` .env.example ` 为 `.env` 并按需修改
   - 数据库连接读取 `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD/DB_SCHEMA`
3. 启动：`python app.py`
4. 访问：`http://localhost:5003`

## 约定与注意
- 仓库文件编码为 UTF-8；在 PowerShell 查看中文内容建议用 `Get-Content -Encoding UTF8 <file>`。
- 尽量保持现有路由/接口/模板结构不变；新增功能优先通过蓝图、service、dao 分层落地。
- 涉及 Excel/Word 导出：优先复用现有 `openpyxl` / `docxtpl` 的实现风格，避免引入新依赖。

