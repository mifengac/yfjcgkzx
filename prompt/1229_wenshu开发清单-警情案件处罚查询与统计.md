# 警情案件处罚查询与统计 - 开发清单

## 一、项目概述

### 1.1 功能描述
在警情案件管理模块新增一个Tab页"警情案件处罚查询与统计"，提供警情案件处罚数据的统计查询、明细查看和导出功能。

### 1.2 开发目录
所有新代码文件位于：`C:\Users\So\Desktop\project\yfjcgkzx1227\jingqing_anjian\`

### 1.3 命名规则
文件前缀：`jqaj_jqajcfcxytj_` (警情案件-警情案件处罚查询与统计)
- `jqaj_jqajcfcxytj_routes.py` - 路由层
- `jqaj_jqajcfcxytj_service.py` - 服务层
- `jqaj_jqajcfcxytj_dao.py` - 数据访问层

---

## 二、页面结构设计

### 2.1 主页面布局

```
┌─────────────────────────────────────────────────────────────────┐
│  Tab导航区域                                                     │
│  [案件查询与统计分析] [警情案件详情] [人员详情] [警情案件总览]     │
│  [警情案件处罚查询与统计]  ← 新增                                │
├─────────────────────────────────────────────────────────────────┤
│  筛选区                                                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 警情类型: [多选下拉框]  开始时间: [2025-01-01 00:00:00]  │    │
│  │ 结束时间: [2025-12-22 00:00:00]  [查询]                  │    │
│  └─────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────┤
│  数据显示区 (jqajcfcxytjs)                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  [导出 ▼] (Hover显示excel/csv)                           │    │
│  │ ┌───────┬──────┬──────┬──────┬──────┬──────┬──────┐     │    │
│  │ │地区   │警情  │行政  │刑事  │治拘  │刑拘  │...   │     │    │
│  │ ├───────┼──────┼──────┼──────┼──────┼──────┼──────┤     │    │
│  │ │云城   │ 123  │ 45   │ 67   │ 12   │ 8    │...   │     │    │
│  │ │云安   │ 234  │ 56   │ 78   │ 23   │ 15   │...   │     │    │
│  │ └───────┴──────┴──────┴──────┴──────┴──────┴──────┘     │    │
│  │ (点击数字弹出明细页面)                                       │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 明细页面布局 (弹窗)

```
┌─────────────────────────────────────────────────────────────────┐
│  数据显示区 (jqajcfcxytj_details)                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  [导出 ▼] (Hover显示excel/csv)                           │    │
│  │ ┌───────────────────────────────────────────────────┐   │    │
│  │ │ SQL查询结果直接展示                                 │   │    │
│  │ │ (jingqings/anjians/wenshus)                        │   │    │
│  │ └───────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                    [关闭]                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 三、数据库设计

### 3.1 涉及的数据表/视图

| 表名/视图名 | 用途 |
|------------|------|
| `case_type_config` | 警情类型配置表 |
| `v_jq_optimized` | 警情优化视图 |
| `mv_zfba_all_ajxx` | 执法办案-案件信息物化视图 |
| `mv_zfba_wenshu` | 执法办案-文书物化视图 |
| `zfba_aj_003` | 执法办案-案件信息表 |
| `zfba_aj_009` | 执法办案-人员信息表 |

### 3.2 SQL查询语句

#### 3.2.1 警情数据查询 (jingqings)
```sql
SELECT *, LEFT(vjo."cmdid", 6) AS "diqu"
FROM "v_jq_optimized" vjo
WHERE vjo."calltime" BETWEEN {tbkssj} AND {jssj}
  AND "leixing" = {leixing}
```

#### 3.2.2 案件数据查询 (anjians)
```sql
SELECT *
FROM "mv_zfba_all_ajxx" mzaa
WHERE mzaa."立案日期" BETWEEN {tbkssj} AND {jssj}
  AND mzaa."案由" SIMILAR TO (
    SELECT ay_pattern
    FROM "case_type_config" ctc
    WHERE ctc."leixing" = {leixing}
  )
```

#### 3.2.3 文书数据查询 (wenshus)
```sql
WITH cfg AS (
    SELECT ay_pattern
    FROM ywdata.case_type_config
    WHERE "leixing" = {leixing}
),
ws_dedup AS (
    SELECT DISTINCT ON (ws.wsywxxid)
        ws.*
    FROM ywdata.mv_zfba_wenshu ws
    WHERE COALESCE(ws.spsj, ws.tfsj) >= {tbkssj}::timestamp
      AND COALESCE(ws.spsj, ws.tfsj) <= {jssj}::timestamp
    ORDER BY ws.wsywxxid, ws.tfsj DESC NULLS LAST
),
aj_dedup AS (
    SELECT DISTINCT ON (aj.asjbh)
        aj.*
    FROM ywdata.zfba_aj_003 aj
    WHERE ('打架斗殴' IS NULL OR '打架斗殴' = '')
      OR EXISTS (
        SELECT 1
        FROM cfg c
        WHERE aj.aymc SIMILAR TO c.ay_pattern
      )
    ORDER BY aj.asjbh, aj.xgsj DESC NULLS LAST
),
base AS (
    SELECT
        LEFT(ws.badwdm, 6) AS region,
        ws.badwmc,
        ws.flws_dxbh,
        ws.flws_bt,
        ws.tfsj,
        ws.spsj,
        ws.asjbh,
        ws.asjmc,
        ws.wsywxxid,
        aj.aymc,
        p.sfjg,
        p.jlts,
        p.fk,
        ws.flws_zlmc,
        ws.flws_dxlxdm,
        ws.flws_dxbxm
    FROM ws_dedup ws
    LEFT JOIN aj_dedup aj ON ws.asjbh = aj.asjbh
    LEFT JOIN ywdata.zfba_aj_009 p ON ws.wsywxxid = p.wsywxxid
)
SELECT
    b.region::text AS region,
    b.badwmc::text AS badwmc,
    b.wsywxxid::TEXT AS wsywxxid,
    b.flws_dxlxdm::TEXT AS dxlxdm,
    b.flws_dxbh::text AS flws_dxbh,
    b.flws_dxbxm::text AS flws_dxbxm,
    b.flws_bt::text AS flws_bt,
    COALESCE(b.spsj, b.tfsj) AS spsj,
    b.asjbh::text AS asjbh,
    b.asjmc::text AS asjmc,
    COALESCE(b.sfjg, '0') AS jinggao,
    COALESCE(b.fk, '0') AS fakuan,
    COALESCE(b.jlts, '0') AS zhiju
FROM base b
WHERE b.aymc IS NOT NULL
```

---

## 四、数据统计逻辑

### 4.1 时间段计算

**输入：** `kssj`, `jssj`
**计算：**
- 同比开始时间 `tbkssj` = `kssj` 年份 - 1
- 同比结束时间 `tbjssj` = `jssj` 年份 - 1

**示例：**
```
kssj = '2025-01-01 00:00:00' → tbkssj = '2024-01-01 00:00:00'
jssj = '2025-12-22 00:00:00' → tbjssj = '2024-12-22 00:00:00'
```

### 4.2 地区映射

| 代码 | 地区 |
|------|------|
| 445302 | 云城 |
| 445303 | 云安 |
| 445381 | 罗定 |
| 445321 | 新兴 |
| 445322 | 郁南 |
| 445300 | 市局 |
| 其他 | 其他 |

**分组依据：**
- `jingqings.diqu` (LEFT(cmdid, 6))
- `anjians.地区`
- `wenshus.region`

### 4.3 统计指标定义

| 指标 | 数据源 | 时间段 | 计算逻辑 |
|------|--------|--------|----------|
| 警情 | jingqings | kssj~jssj | COUNT(DISTINCT caseno) |
| 同比警情 | jingqings | tbkssj~tbjssj | COUNT(DISTINCT caseno) |
| 行政 | anjians | kssj~jssj | COUNT(案件类型='行政') |
| 同比行政 | anjians | tbkssj~tbjssj | COUNT(案件类型='行政') |
| 刑事 | anjians | kssj~jssj | COUNT(案件类型='刑事') |
| 同比刑事 | anjians | tbkssj~tbjssj | COUNT(案件类型='刑事') |
| 治拘 | wenshus | kssj~jssj | COUNT(zhiju!='0') |
| 同比治拘 | wenshus | tbkssj~tbjssj | COUNT(zhiju!='0') |
| 刑拘 | wenshus | kssj~jssj | COUNT(DISTINCT wsywxxid) WHERE flws_bt包含'拘留证' |
| 同比刑拘 | wenshus | tbkssj~tbjssj | COUNT(DISTINCT wsywxxid) WHERE flws_bt包含'拘留证' |
| 起诉 | wenshus | kssj~jssj | COUNT(DISTINCT wsywxxid) WHERE flws_BT包含'起诉意见' |
| 同比起诉 | wenshus | tbkssj~tbjssj | COUNT(DISTINCT wsywxxid) WHERE flws_bt包含'起诉意见' |
| 移送人员 | wenshus | kssj~jssj | COUNT(DISTINCT wsywxxid) WHERE dxlxdm='01' AND flws_bt包含'移送' |
| 同比移送人员 | wenshus | tbkssj~tbjssj | COUNT(DISTINCT wsywxxid) WHERE dxlxdm='01' AND flws_bt包含'移送' |
| 移送案件 | wenshus | kssj~jssj | COUNT(DISTINCT wsywxxid) WHERE dxlxdm='04' AND flws_bt包含'移送' |
| 同比移送案件 | wenshus | tbkssj~tbjssj | COUNT(DISTINCT wsywxxid) WHERE dxlxdm='04' AND flws_bt包含'移送' |

### 4.4 表格字段列表

**汇总表字段 (17个)：**
1. 地区
2. 警情
3. 同比警情
4. 行政
5. 同比行政
6. 刑事
7. 同比刑事
8. 治拘
9. 同比治拘
10. 刑拘
11. 同比刑拘
12. 起诉
13. 同比起诉
14. 移送人员
15. 同比移送人员
16. 移送案件
17. 同比移送案件

---

## 五、文件开发清单

### 5.1 路由层文件

**文件路径：** `jingqing_anjian/routes/jqaj_jqajcfcxytj_routes.py`

**路由清单：**

| 路由 | 方法 | 功能 |
|------|------|------|
| `/jqajcfcxytj` | GET | 警情案件处罚查询与统计主页面 |
| `/jqajcfcxytj/detail` | GET | 明细弹窗页面 |
| `/api/jqajcfcxytj/types` | GET | 获取警情类型列表 |
| `/api/jqajcfcxytj/summary` | POST | 获取汇总统计数据 |
| `/api/jqajcfcxytj/detail` | POST | 获取明细数据 |
| `/api/jqajcfcxytj/export` | POST | 导出数据 |

**请求参数示例：**
```python
# /api/jqajcfcxytj/summary
{
    "leixing": ["类型1", "类型2"],  # 多选
    "kssj": "2025-01-01 00:00:00",
    "jssj": "2025-12-22 00:00:00"
}

# /api/jqajcfcxytj/detail
{
    "leixing": ["类型1"],
    "kssj": "2025-01-01 00:00:00",
    "jssj": "2025-12-22 00:00:00",
    "click_field": "警情",  # 点击的字段名
    "region": "445302"      # 点击的地区代码
}

# /api/jqajcfcxytj/export
{
    "leixing": [...],
    "kssj": "...",
    "jssj": "...",
    "format": "excel" | "csv",
    "data_type": "summary" | "detail",
    "click_field": "...",  # detail时必填
    "region": "..."        # detail时必填
}
```

### 5.2 服务层文件

**文件路径：** `jingqing_anjian/service/jqaj_jqajcfcxytj_service.py`

**方法清单：**

```python
class JqajcfcxytjService:
    def get_case_types(self):
        """获取警情类型列表"""

    def calculate_tb_dates(self, kssj, jssj):
        """计算同比时间段"""

    def get_jingqings(self, tbkssj, jssj, leixing_list):
        """获取警情数据"""

    def get_anjians(self, tbkssj, jssj, leixing_list):
        """获取案件数据"""

    def get_wenshus(self, tbkssj, jssj, leixing_list):
        """获取文书数据"""

    def process_summary_stats(self, kssj, jssj, tbkssj, tbjssj, leixing_list):
        """处理汇总统计数据"""

    def get_detail_data(self, kssj, jssj, leixing_list, click_field, region):
        """获取明细数据"""

    def build_export_data(self, data, columns, format_type):
        """构建导出数据"""
```

### 5.3 数据访问层文件

**文件路径：** `jingqing_anjian/dao/jqaj_jqajcfcxytj_dao.py`

**方法清单：**

```python
class JqajcfcxytjDAO:
    def get_case_types(self):
        """获取警情类型"""
        query = """
            SELECT ctc."leixing"
            FROM "case_type_config" ctc
        """

    def get_jingqings(self, tbkssj, jssj, leixing_list):
        """获取警情数据"""

    def get_anjians(self, tbkssj, jssj, leixing_patterns):
        """获取案件数据"""

    def get_wenshus(self, tbkssj, jssj, leixing_patterns):
        """获取文书数据"""
```

### 5.4 模板文件

**文件路径：** `jingqing_anjian/templates/jqajcfcxytj.html`

**包含内容：**
- 筛选区HTML结构
- 多选下拉框组件
- 数据显示表格
- 导出下拉按钮
- 点击事件处理

**文件路径：** `jingqing_anjian/templates/jqajcfcxytj_detail.html`

**包含内容：**
- 明细数据表格
- 导出按钮
- 关闭按钮

**文件路径：** `jingqing_anjian/templates/jqajcfcxytj.js`

**包含内容：**
- 多选下拉框逻辑
- 查询按钮事件
- 表格点击事件
- 弹窗控制逻辑
- 导出功能调用

---

## 六、接口设计

### 6.1 获取警情类型接口

**请求：**
```http
GET /jingqing_anjian/api/jqajcfcxytj/types
```

**响应：**
```json
{
    "success": true,
    "data": [
        {"leixing": "盗窃"},
        {"leixing": "诈骗"},
        {"leixing": "打架斗殴"}
    ]
}
```

### 6.2 获取汇总统计接口

**请求：**
```http
POST /jingqing_anjian/api/jqajcfcxytj/summary
Content-Type: application/json

{
    "leixing": ["盗窃", "诈骗"],
    "kssj": "2025-01-01 00:00:00",
    "jssj": "2025-12-22 00:00:00"
}
```

**响应：**
```json
{
    "success": true,
    "data": [
        {
            "地区": "云城",
            "警情": 123,
            "同比警情": 110,
            "行政": 45,
            "同比行政": 42,
            "刑事": 67,
            "同比刑事": 58,
            "治拘": 12,
            "同比治拘": 10,
            "刑拘": 8,
            "同比刑拘": 6,
            "起诉": 5,
            "同比起诉": 4,
            "移送人员": 3,
            "同比移送人员": 2,
            "移送案件": 2,
            "同比移送案件": 1
        }
        // ... 其他地区
    ]
}
```

### 6.3 获取明细数据接口

**请求：**
```http
POST /jingqing_anjian/api/jqajcfcxytj/detail
Content-Type: application/json

{
    "leixing": ["盗窃"],
    "kssj": "2025-01-01 00:00:00",
    "jssj": "2025-12-22 00:00:00",
    "click_field": "警情",
    "region": "445302"
}
```

**响应（警情明细）：**
```json
{
    "success": true,
    "columns": ["警情编号", "类型", "报警时间", "地区", "派出所", ...],
    "data": [
        {
            "caseno": "JQ202501010001",
            "leixing": "盗窃",
            "calltime": "2025-01-01 10:30:00",
            ...
        }
    ]
}
```

### 6.4 导出接口

**请求：**
```http
POST /jingqing_anjian/api/jqajcfcxytj/export
Content-Type: application/json

{
    "leixing": ["盗窃"],
    "kssj": "2025-01-01 00:00:00",
    "jssj": "2025-12-22 00:00:00",
    "format": "excel",
    "data_type": "summary"
}
```

**响应：**
- Excel: 文件下载 `警情案件处罚统计.xlsx`
- CSV: 文件下载 `警情案件处罚统计.csv`

---

## 七、前端交互设计

### 7.1 筛选区交互

```javascript
// 多选下拉框
<div class="multi-select">
    <div class="select-trigger">请选择警情类型</div>
    <div class="dropdown-menu">
        <label><input type="checkbox" value="盗窃"> 盗窃</label>
        <label><input type="checkbox" value="诈骗"> 诈骗</label>
        <!-- ... -->
    </div>
</div>

// 日期时间选择器
<input type="datetime-local" id="kssj" step="1">
<input type="datetime-local" id="jssj" step="1">
```

### 7.2 表格点击事件

```javascript
// 点击单元格弹出明细
function handleCellClick(field, region, value) {
    if (value === 0 || value === '-') return;

    const params = {
        leixing: getSelectedTypes(),
        kssj: document.getElementById('kssj').value,
        jssj: document.getElementById('jssj').value,
        click_field: field,
        region: region
    };

    // 打开弹窗
    openDetailModal(params);
}
```

### 7.3 导出功能

```javascript
// 导出下拉菜单
<div class="export-dropdown">
    <button class="export-btn">导出 ▼</button>
    <div class="export-menu">
        <button onclick="exportData('excel')">Excel</button>
        <button onclick="exportData('csv')">CSV</button>
    </div>
</div>

function exportData(format) {
    const params = {
        leixing: getSelectedTypes(),
        kssj: document.getElementById('kssj').value,
        jssj: document.getElementById('jssj').value,
        format: format,
        data_type: currentDataType,
        click_field: currentField,
        region: currentRegion
    };

    fetch('/jingqing_anjian/api/jqajcfcxytj/export', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(params)
    })
    .then(res => res.blob())
    .then(blob => {
        // 下载文件
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `警情案件处罚统计_${format}.${format === 'excel' ? 'xlsx' : 'csv'}`;
        a.click();
    });
}
```

### 7.4 弹窗实现

```javascript
// 弹窗HTML
<div id="detailModal" class="modal">
    <div class="modal-content">
        <div class="modal-header">
            <h3>明细数据</h3>
            <button class="close-btn" onclick="closeModal()">×</button>
        </div>
        <div class="modal-body">
            <iframe id="detailFrame" src="about:blank"></iframe>
        </div>
    </div>
</div>

// 弹窗控制
function openDetailModal(params) {
    const modal = document.getElementById('detailModal');
    const frame = document.getElementById('detailFrame');
    const url = `/jingqing_anjian/jqajcfcxytj/detail?${new URLSearchParams(params)}`;
    frame.src = url;
    modal.style.display = 'flex';
}

function closeModal() {
    document.getElementById('detailModal').style.display = 'none';
}
```

---

## 八、开发步骤

### 阶段一：数据访问层 (DAO)

- [ ] 创建 `jqaj_jqajcfcxytj_dao.py`
- [ ] 实现 `get_case_types()` 方法
- [ ] 实现 `get_jingqings()` 方法
- [ ] 实现 `get_anjians()` 方法
- [ ] 实现 `get_wenshus()` 方法
- [ ] 编写单元测试

### 阶段二：服务层 (Service)

- [ ] 创建 `jqaj_jqajcfcxytj_service.py`
- [ ] 实现时间段计算逻辑
- [ ] 实现汇总统计二次计算逻辑
- [ ] 实现明细数据查询逻辑
- [ ] 实现导出数据构建逻辑
- [ ] 编写单元测试

### 阶段三：路由层 (Routes)

- [ ] 创建 `jqaj_jqajcfcxytj_routes.py`
- [ ] 注册蓝图到 `app.py`
- [ ] 实现页面路由
- [ ] 实现API接口路由
- [ ] 实现导出路由

### 阶段四：模板文件

- [ ] 创建 `jqajcfcxytj.html`
- [ ] 创建 `jqajcfcxytj_detail.html`
- [ ] 创建 `jqajcfcxytj.js`
- [ ] 实现筛选区组件
- [ ] 实现数据显示表格
- [ ] 实现导出按钮交互
- [ ] 实现弹窗逻辑

### 阶段五：集成测试

- [ ] 测试警情类型加载
- [ ] 测试汇总统计查询
- [ ] 测试明细数据查询
- [ ] 测试Excel导出
- [ ] 测试CSV导出
- [ ] 测试多选警情类型
- [ ] 测试同比计算准确性

---

## 九、注意事项

### 9.1 安全性

- 使用参数化查询防止SQL注入
- 验证输入时间格式
- 限制查询时间范围（建议不超过1年）

### 9.2 性能优化

- 文书查询使用CTE去重
- 考虑添加索引
- 大数据量时使用分页

### 9.3 兼容性

- 确保与现有Tab页切换兼容
- 复用现有导出工具函数
- 遵循项目编码规范（UTF-8）

### 9.4 错误处理

- 数据库查询异常捕获
- 前端请求失败提示
- 导出失败友好提示

---

## 十、参考文件

### 10.1 项目参考

- 路由示例：`jingqing_anjian/routes/jingqing_anjian_routes.py`
- 服务示例：`jingqing_anjian/service/jqajzl_service.py`
- DAO示例：`jingqing_anjian/dao/case_dao.py`
- 模板示例：`jingqing_anjian/templates/jqajzl.html`

### 10.2 数据库配置

- 配置文件：`gonggong/config/database.py`
- 连接函数：`get_database_connection()`
- 查询函数：`execute_query()`

---

*文档生成时间: 2024-12-29*
*项目路径: C:\Users\So\Desktop\project\yfjcgkzx1227*
