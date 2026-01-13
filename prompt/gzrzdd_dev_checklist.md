# 工作日志督导（gzrzdd）开发清单

需求来源：`C:\Users\So\Desktop\yfjcgkzx0111\prompt\0113_gzrz_jd.md`

## 0. 需你确认的问题（确认后我再开始开发）
1. `xxxxx` 倒序排序字段到底是哪一列？  
   - 我建议默认按 `列管时间` 倒序；如果你 SQL 里字段名不同，请在 SQL 里 `AS 列管时间` 或告诉我实际列名。
2. 权限表 `ywdata.jcgkzx_permission.module` 里，新模块的取值是否就填 `工作日志督导`？  
   - 我将按该值做权限校验与主页按钮展示；如你想用别的值请告知。

## 1. 模块结构（新增）
在项目根目录新增模块目录 `gzrzdd/`（参考 `xunfang/`、`zhizong/` 结构）：
- `gzrzdd/routes/gzrzdd_routes.py`：蓝图与路由（页面、统计API、明细API、导出）
- `gzrzdd/service/gzrzdd_service.py`：核心计算逻辑（取最新N条 + 清洗 + TF‑IDF重复簇 + 透视表）
- `gzrzdd/dao/gzrzdd_dao.py`：数据库查询（复用 `gonggong.config.database.get_database_connection`）
- `gzrzdd/templates/gzrzdd.html`：页面（count/chongfudu/SQL 输入、统计按钮、结果表格、导出）
- `gzrzdd/templates/gzrzdd_detail.html`：明细页（对应派出所+分局的重复日志列表、导出）
- `static/js/gzrzdd.js`：主页面交互（发起统计、渲染交叉表、跳转明细、导出）
- `static/js/gzrzdd_detail.js`：明细页交互（加载数据、导出）

## 2. 接入项目入口
修改 `C:\Users\So\Desktop\yfjcgkzx0111\app.py`：
1. 导入并注册新蓝图 `gzrzdd_bp`（`url_prefix="/gzrzdd"`）
2. 在 `main()` 的模块映射里新增一项：
   - module=`工作日志督导` -> endpoint=`gzrzdd.index` label=`工作日志督导`
3. （可选）在 `MODULE_DEFINITIONS` 里也补一项（如果该列表未来会用到）

## 3. 权限控制
参考 `xunfang/routes/xunfang_routes.py`：
- `@gzrzdd_bp.before_request`：
  - 未登录则跳转 `/login`
  - 校验 `jcgkzx_permission` 是否存在 `username=当前用户` 且 `module='工作日志督导'`

## 4. 页面与接口
### 4.1 页面
1. `GET /gzrzdd/`：工作日志督导首页
2. `GET /gzrzdd/detail`：明细页（参数：result_id、分局名称、所属派出所）

### 4.2 API
1. `POST /gzrzdd/api/stats`
   - 入参：`count`（默认5）、`chongfudu`（默认80）、`sql`（用户提供）
   - 返回：交叉表（行=所属派出所，列=分局名称，值=重复日志条数）+ `result_id`
2. `GET /gzrzdd/api/detail`
   - 入参：`result_id`、`branch`、`station`
   - 返回：该单元格对应的重复日志明细（含 `序号`、`重复度`）
3. 导出
   - `GET /gzrzdd/download/summary?result_id=...&format=xlsx|csv&count=...`
     - 文件名：`各地最近{count}条工作日志重复度{时间戳}.{ext}`
   - `GET /gzrzdd/download/detail?result_id=...&branch=...&station=...&format=xlsx|csv&count=...`
     - 文件名：`最近{count}条重复工作日志详情{时间戳}.{ext}`

## 5. 数据处理逻辑（与 `prompt/0113_tf-idf_rz.py` 对齐）
### 5.1 SQL 拉取数据
- 使用 `gonggong.config.database.get_database_connection()`
- 仅允许 `SELECT/WITH`（基础安全校验：禁止分号、禁止 DDL/DML 关键字）
- SQL 结果必须包含列（字段名可通过 SQL `AS` 对齐）：
  - `证件号码`
  - `工作日志工作情况说明`
  - `分局名称`
  - `所属派出所`
  - `列管时间`（或你确认的 `xxxxx` 字段）
  - （可选）`姓名`

### 5.2 取最新 N 条
- 按 `证件号码` 分组
- 组内按排序字段倒序
- 取最新 `count` 条

### 5.3 清洗（匹配前）
- 删除文本开头日期时间前缀（`2025年1月2日上午，...` / `2024年2月2日...` / `2025-01-02 08:30...`）
- 用同一行的 `所属派出所`、`姓名` 对文本做替换为空

### 5.4 相似度与重复簇
- TF‑IDF（脚本内置实现）+ 余弦相似度
- 相似度 >= `chongfudu/100` 建边，取连通分量为重复簇
- 仅保留簇大小 >= 2 的记录
- 为每个簇生成 `序号`（1,2,3...），并记录该簇的 `重复度`（簇内最大相似度，百分比）

### 5.5 交叉表
- 基于“重复日志记录”统计透视表：
  - 行：`所属派出所`
  - 列：`分局名称`
  - 值：重复日志记录条数（count）

## 6. 缓存与性能
- 统计结果存内存缓存（`result_id -> 数据`），设置 TTL（例如 2 小时），用于明细与导出复用
- 每次 stats 计算前做一次缓存清理

## 7. 验收点
1. 首页按钮出现：`工作日志督导`，可跳转进入
2. 输入 `count/chongfudu/sql` 后可生成交叉表
3. 单元格数字可点击打开明细页，展示对应记录
4. 汇总/明细均可导出 `xlsx/csv` 且文件名符合要求

