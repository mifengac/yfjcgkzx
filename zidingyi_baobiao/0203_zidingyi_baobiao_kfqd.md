# 📌 自定义-报表 · 后端开发清单（3 张表版）

---

## 一、项目初始化（基础框架）

### 1️⃣ 项目结构

```
app/
├── main.py
├── core/
│   ├── config.py
│   ├── db.py
│   ├── security.py
│   └── exceptions.py
├── models/
│   ├── data_source.py
│   ├── dataset.py
│   └── module_def.py
├── schemas/
│   ├── module_config.py
│   └── request.py
├── services/
│   ├── datasource_service.py
│   ├── dataset_service.py
│   ├── module_service.py
│   └── query_executor.py
├── api/
│   ├── datasource.py
│   ├── dataset.py
│   └── module.py
└── utils/
    ├── sql_validator.py
    ├── crypto.py
    ├── export.py
    └── logger.py
```

---

## 二、数据库与基础能力

### 2️⃣ 数据库连接

* 使用 SQLAlchemy 2.x
* 支持人大金仓 V8（PG 协议）
* 每个 data_source 独立 engine（连接池）

**验收：**

* 能成功连金仓
* 能执行简单 SELECT

---

### 3️⃣ 密码加解密

* 使用 `cryptography.fernet`
* data_source.password_enc 加密存储
* 明文仅在运行时解密

**验收：**

* DB 中不可见明文密码
* 连接成功

---

## 三、DataSource 模块

### 4️⃣ 数据源管理 API

**接口：**

* `POST /datasource` 新增
* `PUT /datasource/{id}` 修改
* `GET /datasource` 列表
* `POST /datasource/{id}/test` 测试连接

**关键逻辑：**

* 校验 db_type
* 连接测试超时控制（5s）

**验收：**

* 新增后可测试连接成功/失败
* 错误信息可读

---

## 四、Dataset（SQL 模板）模块

### 5️⃣ Dataset 管理 API

**接口：**

* `POST /dataset`
* `PUT /dataset/{id}`
* `GET /dataset`
* `POST /dataset/{id}/preview`

---

### 6️⃣ SQL 校验器（重点）

**保存 & 执行前都要走**

**规则：**

* 允许：`SELECT` / `WITH`
* 禁止关键字：

  * insert, update, delete, drop, alter, truncate
  * ;  do $$  call exec
* 必须使用命名参数（`:param`）

**验收：**

* CTE SQL 可通过
* 非 SELECT 被拒绝

---

### 7️⃣ SQL 预览执行

**行为：**

* 自动包一层：

```sql
SELECT * FROM ( <SQL_TEMPLATE> ) t LIMIT 10
```

* 参数从请求体传

**验收：**

* 返回列名 + 数据
* 错误信息明确

---

## 五、Module（Tab）模块

### 8️⃣ Module 管理 API

**接口：**

* `POST /module`
* `PUT /module/{id}`
* `GET /module`
* `GET /module/{tab_key}/config`

---

### 9️⃣ config_json 校验（Pydantic）

**校验内容：**

* datasets.dataset_id 存在
* dimension_mapping 必须是 `dim → field`
* filters/groups/metrics/columns 结构正确
* 至少一个时间类过滤（between）

**验收：**

* 错误配置直接返回原因
* 正确配置可保存

---

## 六、查询执行核心（最重要）

### 🔥 10️⃣ Query Executor（核心模块）

**输入：**

* module_id / tab_key
* 查询参数（filters）

**执行流程：**

1. 解析 module.config_json
2. 对每个 dataset：

   * 获取 SQL
   * 生成参数（dim → field）
   * 校验参数合法性
   * 执行 SQL
3. 按 merge_rule 合并结果
4. 返回统一结果结构

**合并规则：**

* `merge_by_dimension` → Python groupby + sum/count
* `append` → 直接拼接

**验收：**

* 多 dataset 同维度正确合并
* 字段不一致也能对齐

---

### 11️⃣ 强制运行规则

* 时间过滤必填
* 自动限制 max_rows
* 非导出模式强制 LIMIT

**验收：**

* 不传时间直接拒绝
* 大查询被拦截

---

## 七、导出模块

### 12️⃣ 导出接口

**接口：**

* `POST /module/{id}/export`

**功能：**

* CSV
* XLSX（openpyxl）

**限制：**

* 行数 ≤ N（如 100k）
* 超限报错

**验收：**

* 文件可正常下载
* 字段顺序与 columns 配置一致

---

## 八、日志 & 审计（最简）

### 13️⃣ 操作日志

**记录：**

* 新增/修改 dataset
* 新增/修改 module
* 查询 & 导出

**字段：**

* operator
* action
* target
* params
* time

**验收：**

* 可按时间追溯

---

## 九、错误与安全

### 14️⃣ 统一异常处理

* SQL 错误
* 参数错误
* 超时
* 权限不足（预留）

### 15️⃣ SQL 注入防护

* 禁止字符串拼 SQL
* 参数全部绑定

---

## 十、最终验收清单（你验收 Codex 代码用）

* [ ] CTE SQL 可正常保存 & 执行
* [ ] 一个 Tab 可挂多个 dataset
* [ ] 同一维度可合并展示
* [ ] 时间条件必填
* [ ] 支持 CSV / XLSX 导出
* [ ] SQL 非 SELECT 无法执行
* [ ] 数据源密码不明文

---

## 🔚 给 Codex 的一句话提示（建议你一起发）

> “这是一个**公安研判用的自定义驱动报表系统**，所有 SQL 都必须是安全的 SELECT / WITH，禁止任何 DML；系统核心是 module.config_json + dataset SQL 模板，多数据源结果要按维度合并。”

---

如果你愿意，我下一步可以**直接帮你生成 Codex Prompt（一步到位）**，把每个模块该写什么、注意什么都写清楚，Codex 出代码成功率会更高。
你要的话，回复我一句：**“帮我写 Codex Prompt”**。
===
你是一个资深 Python 后端工程师，请为我生成一个【自定义驱动报表系统】的 Flask 后端代码。

【技术栈与运行环境】
- Python 3.12
- Flask 2.x
- Flask Blueprint
- SQLAlchemy 2.x
- 数据库：人大金仓 Kingbase V8（PostgreSQL 协议）
- 操作系统：Windows 10
- 仅后端，不需要前端代码

【业务背景】
系统用于公安治安数据研判。用户通过 Web 页面配置 SQL 模板和模块（Tab），系统根据配置动态生成查询结果并支持导出。
业务经常变化，要求“改配置不改代码”。

【数据库表（已存在，不要创建新表）】
1）data_source
2）dataset
3）module_def

禁止新增其他业务表（日志可用文件或内存方式简化实现）。

【核心能力】
1）支持配置 SQL 模板（SELECT / WITH 语句）
2）支持 CTE（WITH）临时表
3）支持多数据源、多 dataset 合并
4）支持通过“语义维度”合并不同字段名的数据
5）支持过滤、分组、指标、列配置
6）支持 CSV / XLSX 导出

【重要安全约束（必须严格遵守）】
- SQL 只允许 SELECT 或 WITH 开头
- 严禁 insert / update / delete / drop / alter / truncate / ; / do $$ / call / exec
- 所有 SQL 参数必须使用命名参数（:param）
- 禁止字符串拼 SQL
- 时间范围过滤必须必填（between）
- 非导出查询必须自动 LIMIT
- dataset 设置 timeout_ms 与 max_rows

【模块配置（module_def.config_json 结构，必须支持）】
{
  "datasets": [
    {
      "dataset_id": 1,
      "alias": "110警情",
      "dimension_mapping": {
        "ALARM_TYPE": "alarm_type",
        "ALARM_TIME": "alarm_time",
        "DEPT_CODE": "dept_code"
      }
    }
  ],
  "filters": [
    {
      "dim": "ALARM_TIME",
      "op": "between",
      "control": "date_range",
      "required": true
    }
  ],
  "groups": ["ALARM_TYPE"],
  "metrics": [
    {
      "key": "cnt",
      "label": "警情数",
      "agg": "count"
    }
  ],
  "columns": [
    {"type": "dimension", "dim": "ALARM_TYPE", "label": "警情类别"},
    {"type": "metric", "key": "cnt", "label": "警情数"}
  ],
  "merge_rule": "merge_by_dimension",
  "export": {
    "allow": true,
    "formats": ["xlsx", "csv"]
  }
}

【后端必须实现的功能模块】
1）数据库连接模块（支持 Kingbase）
2）数据源管理（测试连接、密码加解密）
3）Dataset 管理
   - SQL 校验
   - SQL 预览（自动 LIMIT 10）
4）Module 管理
   - config_json 结构校验（可使用 Marshmallow 或手写校验）
5）查询执行引擎（最核心）
   - 多 dataset 执行
   - 维度字段映射
   - Python 层合并结果
6）导出模块
   - CSV（标准库）
   - XLSX（openpyxl）

【API 要求（示例即可）】
- POST /datasource
- POST /datasource/<id>/test
- POST /dataset
- POST /dataset/<id>/preview
- POST /module
- GET  /module/<tab_key>/query
- POST /module/<tab_key>/export

【代码结构要求】
- 使用 Flask Blueprint
- 清晰的分层（models / services / api / utils）
- 所有关键函数必须有注释
- 所有异常必须有可读错误信息
- QueryExecutor 必须单独成文件

【输出要求】
- 输出完整 Flask 项目代码
- 每个文件用清晰的文件名标注
- 代码可直接运行
- 不要输出解释性文字，只输出代码

