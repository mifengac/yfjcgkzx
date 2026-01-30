# 开发清单：综查（未成年人相关三张表）

目标：按需求调整“加强监督教育/责令接受家庭教育指导通知书”抓取参数；并在 HQZCSJ 模块内对 3 张落库表的查询统一追加“审批通过 + 未删除”过滤条件。

## 影响范围（已定位）
- 抓取脚本：`hqzcsj/0123_fetch_data.py`
  - 内置请求参数：`DEFAULT_REQUEST_FORMS["加强监督教育/责令接受家庭教育指导通知书"]`
- HQZCSJ 综查服务：`hqzcsj/service/zongcha_service.py`
  - 该服务内有 3 张表的 job 定义（表名/主键/时间字段/单位字段）
- 需要进一步确认：HQZCSJ 内实际“查询/展示/导出”这 3 张表数据的 SQL/DAO/route 位置（当前代码里未见直接 `SELECT ... FROM zq_zfba_*` 的硬编码，可能是通用查询/动态 SQL）。

## 需求拆解
### 1) 修改抓取参数（0123_fetch_data.py）
把“加强监督教育/责令接受家庭教育指导通知书”这组请求参数改为：
- `json`：`{"paramArray":[{"conditions":[],"tabId":"1782350085472952324","tabCode":"jqjhjyzljsjtjyzdtzs","domainId":"11"}]}`
- `domainId`：`11`
- `resultTabId`：`1782350085472952324`
- `resultTabCode`：`jqjhjyzljsjtjyzdtzs`
- `resultTableName`：`加强监督教育/责令接受家庭教育指导通知书`
- `tabId`：`1782350085472952324`
- `pageSize`：`10`
- `pageNumber`：`1`
- `sortColumns`：空字符串

验收点：脚本发起请求时，上述字段与需求一致。

### 2) 3 张落库表统一追加过滤条件（HQZCSJ 模块）
对 HQZCSJ 中所有读取以下表的查询，统一增加过滤条件：
1. `zq_zfba_zlwcnrzstdxwgftzs`
   - `AND zltzs_wszt='审批通过' AND zltzs_isdel='0'`
2. `zq_zfba_xjs`
   - `AND xjs_wszt='审批通过' AND xjs_isdel='0'`
3. `zq_zfba_jtjyzdtzs`
   - `AND jqjhjyzljsjtjyzdtzs_wszt='审批通过' AND jqjhjyzljsjtjyzdtzs_isdel_dm='0'`

验收点：同一查询条件下，前端/导出/统计中这 3 张表只包含“审批通过且未删除”的记录。

## 实施步骤（Checklist）
- [ ] 在 `hqzcsj/0123_fetch_data.py` 更新上述“家庭教育指导通知书”请求参数（注意该文件里只有 1 处该 label 的默认参数）。
- [ ] 在 HQZCSJ 模块内搜寻所有查询 `zq_zfba_xjs` / `zq_zfba_zlwcnrzstdxwgftzs` / `zq_zfba_jtjyzdtzs` 的 SQL/构造器：
  - [ ] 若是通用查询（传 tableName 动态拼接），在通用层按 tableName 分支追加对应 `AND ...`。
  - [ ] 若是各自 DAO/SQL，逐条在 `WHERE` 后追加对应 `AND ...`。
- [ ] 回归验证（建议最小 3 组）：
  - [ ] 分别对 3 张表做一次“全量查询/导出”，确认过滤生效。
  - [ ] 人工抽样：找一条 `wszt != '审批通过'` 或 `isdel != '0'` 的记录，确认不再出现。

## 口径确认（已确认）
- “家庭教育指导通知书”字段前缀：使用 `jqjhjyzljsjtjyzdtzs_*`
- `wszt` 过滤：使用中文值 `审批通过`
- 提醒：抓取参数将 `conditions` 置空会导致脚本抓取“全量数据”（可能会非常慢/数据量巨大）

## 建议（可选）
- 若目标只是“只抓审批通过且未删除”，更推荐把过滤放到抓取请求（API conditions）里，而不是抓全量后再在查询侧过滤（节省时间与存储）。
- 若必须抓全量，建议给 `hqzcsj/0123_fetch_data.py` 增加可配置的时间范围/单位/状态过滤参数，避免生产环境一次性全量拉取。

json: {"paramArray":[{"conditions":[],"tabId":"1782315945839075392","tabCode":"zltzs","domainId":"11"}]}
domainId: 11
resultTabId: 1782315945839075392
resultTabCode: zltzs
resultTableName: 责令未成年人遵守特定行为规范通知书
tabId: 1782315945839075392
pageSize: 10
pageNumber: 1
sortColumns: 

json: {"paramArray":[{"conditions":[],"tabId":"1782350085472952324","tabCode":"jqjhjyzljsjtjyzdtzs","domainId":"11"}]}
domainId: 11
resultTabId: 1782350085472952324
resultTabCode: jqjhjyzljsjtjyzdtzs
resultTableName: 加强监督教育/责令接受家庭教育指导通知书
tabId: 1782350085472952324
pageSize: 10
pageNumber: 1
sortColumns: 

json: {"paramArray":[{"conditions":[],"tabId":"1782354546966700043","tabCode":"xjs","domainId":"11"}]}
domainId: 11
resultTabId: 1782354546966700043
resultTabCode: xjs
resultTableName: 训诫书（未成年人）
tabId: 1782354546966700043
pageSize: 10
pageNumber: 1
sortColumns: 