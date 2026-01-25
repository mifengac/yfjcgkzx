# 未成年人（打架斗殴）模块开发清单

目标：在现有 Flask 项目中新增模块“未成年人(打架斗殴)”，相关代码均落在 `weichengnianren-djdo/` 目录内，实现大屏页面 + 6 个指标数据查询/可视化 + 导出/导入能力。

## 0. 关键确认项（开工前对齐）
- 模块权限名（写入 `ywdata.jcgkzx_permission.module`）：使用 `未成年人(打架斗殴)`。
- 新模块挂载地址：使用 `/weichengnianren-djdo`（需要在 `app.py` 注册蓝图，虽不在本目录，但属于必需接入点）。
- “导出总览 PDF”：使用 `reportlab` 后端生成（实现时需在 `requirements.txt` 增加依赖并完成接口）。
- 时间范围边界：按闭区间处理（即 `BETWEEN start_time AND end_time`，包含 end_time）。

## 1. 目录结构与接入
- 新增蓝图：`weichengnianren-djdo/routes/wcnr_djdo_routes.py`
  - `before_request` 做登录 + 权限校验（参考 `weichengnianren/routes/wcnr_routes.py` 风格）。
  - 页面路由：GET 大屏页面。
  - API 路由：6 个指标数据接口、导出接口、导入接口。
- 新增 service/dao 分层（保持现有项目习惯）：
  - `weichengnianren-djdo/service/*.py`：聚合/计算/导出逻辑。
  - `weichengnianren-djdo/dao/*.py`：SQL 执行与结果映射。
- 静态资源：
  - `weichengnianren-djdo/static/chart.min.js`（Chart.js 4.4.1，页面引用走 blueprint static 或复制到全局 static）。
  - `weichengnianren-djdo/static/wcnr_djdo.js`：页面逻辑（请求、渲染、排序、下载、导入）。
  - `weichengnianren-djdo/static/wcnr_djdo.css`：大屏布局样式。
- 模板：
  - `weichengnianren-djdo/templates/wcnr_djdo/index.html`

## 2. 页面功能清单（大屏）
- 页面标题：`未成年人打架斗殴六项指标监测`。
- 全局时间控件（左上角）：
  - 格式：`YYYY-MM-DD HH:MM:SS`
  - 默认：开始=当天-7天 00:00:00；结束=当天 00:00:00（例如今天 `2026-01-23` 则为 `2026-01-16 00:00:00` 到 `2026-01-23 00:00:00`）。
  - 全局时间变化时：默认联动刷新 6 个板块（如需“每块独立时间”则保持板块控件优先级更高）。
- 6 个板块布局：2 行 x 3 列
  - 警情转案率
  - 采取矫治教育措施率
  - 涉刑人员送学率
  - 责令加强监护率
  - 场所发案率
  - 纳管人员再犯率
- 每个板块组件：
  - 板块内时间控件（同格式/同默认值）。
  - 柱状图：地区固定为 `云城/云安/罗定/新兴/郁南/全市`；每地区 3 个柱子（见各指标定义）。
  - 排序按钮：可选择按 3 个柱子中的任意一个排序（前端排序即可）。
  - 下载按钮：下载本板块 xlsx，文件名：`{开始时间}-{结束时间}{小版块标题}{时间戳}.xlsx`
  - 图例点击可显示/隐藏柱子（Chart.js 内置 legend click 或自定义按钮）。
- 页面右上角按钮：
  - 导出总览（PDF）
  - 导出详情（xlsx，6 个 sheet）
  - 导入送校数据（只允许 xls，校验 sheet=累计招生 且第3行表头一致，执行 `0125_wcnr_sfzxx_import.py` 的导入逻辑）

## 3. 后端接口清单（建议）
说明：接口风格建议与现有模块一致：返回 `{success, data, ...}`。

### 3.1 6 个指标数据接口（每个指标至少 2 份数据）
- `GET /api/wcnr_djdo/metric/{metric_key}`
  - 入参：`start_time`, `end_time`（字符串），可选 `sort_by`, `sort_dir`
  - 出参：
    - `chart`: 6 地区 x 3 指标值（给柱状图用）
    - `table`: 明细列表（给表格/导出用）
  - metric_key 建议：`jq_za`, `jzjy`, `sx_sx`, `zljqjh`, `cs_fa`, `ng_zf`

### 3.2 单板块下载 xlsx
- `GET /api/wcnr_djdo/export/xlsx?metric_key=...&start_time=...&end_time=...`
  - 生成 1 个 sheet，包含：
    - 柱状图数据表（按地区列出 3 个柱子）
    - 明细表（按 SQL 返回字段）

### 3.3 导出详情（xlsx，6 个 sheet）
- `GET /api/wcnr_djdo/export/details?start_time=...&end_time=...`
  - 6 个 sheet：`警情转案率/采取矫治教育措施率/涉刑人员送学率/责令加强监护率/场所发案率/纳管人员再犯率`
  - 每个 sheet 放该指标的“明细表”（如需同时放图表数据则追加到表尾或新增一个“chart_”sheet，需确认）。

### 3.4 导出总览（PDF）
- `GET /api/wcnr_djdo/export/overview_pdf?start_time=...&end_time=...`
  - 若采用“前端传图”方案：改为 `POST`，body 传 6 个图的 base64 + 6 份表格（或仅图 + 汇总表）。

### 3.5 导入送校数据（xls）
- `POST /api/wcnr_djdo/import/sx_xls`
  - 校验：扩展名必须 `.xls`；`sheet` 必须包含 `累计招生`；第 3 行表头必须匹配脚本预期。
  - 执行：复用 `weichengnianren-djdo/0125_wcnr_sfzxx_import.py` 中的读取/建表/UPSERT 逻辑（避免 subprocess）。
  - 出参：导入行数、跳过行数（编号空）、更新/新增统计（如需可通过额外 SQL 统计）。

## 4. 6 个指标的数据实现清单（后端）
### 4.1 警情转案率
- SQL：按文档中 `zq_kshddpt_dsjfx_jq` LEFT JOIN `mv_zfba_ajxx`
- 地区：通过 `jq.cmdname` 包含关系映射到 5 地区；全市=全部
- 三柱：
  - 警情数：`jq.caseno` count
  - 案件数：`mza.案件编号` count（空不计）
  - 转案率：案件/警情*100（2位小数）
- 明细字段：SQL SELECT 的全部列

### 4.2 采取矫治教育措施率
- SQL：按文档 with 结构（minor_fight / target_aj / doc_hit / baxgry_json）
- 地区：SQL 内 `地区` 字段；全市=全部
- 三柱：
  - 应采取矫治教育措施人数：全量计数
  - 已采取矫治教育措施人数：`是否开具文书='是'` 计数
  - 采取矫治教育措施率：已/应*100（2位小数）
- 明细字段：SQL 最终 SELECT 的全部列

### 4.3 涉刑人员送学率
- SQL：按文档（依赖 `zq_wcnr_sfzxx`，并以 `sfzhm=zjhm` + `rx_time>立案日期` 命中）
- 三柱：
  - 符合涉刑人员送学人数：全量计数
  - 实际送学人数：`是否送校='是'` 计数
  - 涉刑人员送学率：实际/符合*100（2位小数）
- 导入依赖：确保 `ywdata.zq_wcnr_sfzxx` 由导入按钮维护更新

### 4.4 责令加强监护率
- SQL：按文档（`ywdata.zq_zfba_jtjyzdtzs` 命中文书）
- 三柱：
  - 应责令加强监护人数：全量计数
  - 已责令加强监护人数：`是否开具文书='是'` 计数
  - 责令加强监护率：已/应*100（2位小数）

### 4.5 场所发案率（含地址模型分类）
- SQL：按文档（aj_list + 联系人）
- 追加处理：对字段“案件发生地址名称”（需确认 SQL 中的列名实际为哪个）使用 `xunfang/service/jiemiansanlei_service.py::predict_addresses` 分类，新增列 `分类结果`（可同时保留 `pred_prob`）
- 三柱：
  - 娱乐场所案件数：`分类结果='重点管控场所'` 计数
  - 案件数：全量计数
  - 场所发案率：娱乐场所/案件数*100（2位小数）
- 性能：模型推理建议批量（已有 batch=64）；可在 service 层做简单缓存（按地址文本缓存 label/prob）。

### 4.6 纳管人员再犯率
- SQL：按文档（fight_suspect + `stdata.b_zdry_ryxx`）
- 三柱：
  - 列管人数：全量计数
  - 再犯人数：`是否再犯='是'` 计数
  - 再犯率：再犯/列管*100（2位小数）

## 5. 通用实现规范（前后端一致）
- 地区顺序固定：`云城/云安/罗定/新兴/郁南/全市`
- 百分比统一：分母为 0 时显示 `0.00`（避免除零）；保留 2 位小数
- 时间字符串解析：统一用 `YYYY-MM-DD HH:MM:SS`，后端入参校验失败返回友好错误
- 下载文件名：需替换 Windows 不允许字符（如 `:` → `-`）

## 6. 送校数据导入（xls）注意点
- 只允许 `.xls`：前端先校验扩展名；后端再次校验
- 表头校验：必须为第 3 行，列名与脚本 `EXPECTED_HEADERS` 一致（忽略空格/逗号差异）
- 入库表：`ywdata.zq_wcnr_sfzxx`，主键 `bh(编号)`，导入逻辑为 UPSERT

## 7. 联调与验收清单
- 页面加载：默认时间范围正确（按“当天 00:00:00”计算）
- 6 个板块：数据能出、图能画、图例可隐藏/显示、排序生效
- 6 个单板块下载：xlsx 文件名符合规则，内容含图表数据+明细（或按约定）
- 导出详情：6 sheet 名称正确、字段完整
- 导出总览：PDF 文件名正确，内容包含 6 个板块图与表（按选定方案验收）
- 导入送校数据：只收 xls；表头不匹配能提示；匹配则入库并返回“新增/更新/跳过”统计
