# 精神障碍患者预警模块 - 权限配置说明

## 模块信息

- **模块名称**: 精神障碍患者预警
- **URL前缀**: `/jszahzyj`
- **权限标识**: `精神障碍`

## 权限配置步骤

### 1. 在权限表中添加模块权限

需要在 `ywdata.jcgkzx_permission` 表中为用户添加"精神障碍"模块的访问权限。

#### SQL示例

```sql
-- 为指定用户添加精神障碍模块权限
INSERT INTO "ywdata"."jcgkzx_permission" (username, module)
VALUES ('用户名', '精神障碍');

-- 示例：为admin用户添加权限
INSERT INTO "ywdata"."jcgkzx_permission" (username, module)
VALUES ('admin', '精神障碍');

-- 批量为多个用户添加权限
INSERT INTO "ywdata"."jcgkzx_permission" (username, module)
VALUES
    ('user1', '精神障碍'),
    ('user2', '精神障碍'),
    ('user3', '精神障碍');
```

### 2. 查询现有权限

```sql
-- 查看所有拥有精神障碍模块权限的用户
SELECT username, module
FROM "ywdata"."jcgkzx_permission"
WHERE module = '精神障碍';

-- 查看指定用户的所有权限
SELECT username, module
FROM "ywdata"."jcgkzx_permission"
WHERE username = '用户名';
```

### 3. 删除权限

```sql
-- 删除指定用户的精神障碍模块权限
DELETE FROM "ywdata"."jcgkzx_permission"
WHERE username = '用户名' AND module = '精神障碍';
```

## 访问说明

### 访问地址

- **开发环境**: `http://localhost:5003/jszahzyj`
- **生产环境**: `http://<服务器地址>:5003/jszahzyj`

### 访问流程

1. 用户登录系统
2. 系统检查用户是否拥有"精神障碍"模块权限
3. 如果有权限，主菜单会显示"精神障碍患者预警"入口
4. 点击入口即可访问模块

### 权限检查逻辑

模块使用 `@before_request` 装饰器进行权限检查：

```python
@jszahzyj_bp.before_request
def _check_access() -> None:
    """
    访问控制：
    - 需已登录
    - 且在 jcgkzx_permission 中拥有 module = '精神障碍' 的权限
    """
    if not session.get("username"):
        return redirect(url_for("login"))

    # 查询权限表
    conn = get_database_connection()
    with conn.cursor() as cur:
        cur.execute(
            'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
            (session["username"], "精神障碍"),
        )
        row = cur.fetchone()
    conn.close()

    if not row:
        abort(403)  # 无权限，返回403错误
```

## 常见问题

### Q1: 用户登录后看不到"精神障碍患者预警"入口？

**A**: 检查该用户是否在权限表中有对应记录：

```sql
SELECT * FROM "ywdata"."jcgkzx_permission"
WHERE username = '用户名' AND module = '精神障碍';
```

如果没有记录，需要添加权限。

### Q2: 访问模块时提示403错误？

**A**: 说明权限检查失败，可能原因：
1. 用户未登录
2. 用户没有"精神障碍"模块权限
3. 权限表中的module字段值不匹配（注意大小写和空格）

### Q3: 如何批量导入用户权限？

**A**: 可以使用CSV文件批量导入：

```sql
-- 假设有CSV文件包含用户名列表
COPY "ywdata"."jcgkzx_permission" (username, module)
FROM '/path/to/users.csv'
WITH (FORMAT csv, HEADER true);
```

或使用Python脚本批量插入：

```python
import psycopg2

conn = psycopg2.connect(...)
cur = conn.cursor()

users = ['user1', 'user2', 'user3', ...]
for user in users:
    cur.execute(
        'INSERT INTO "ywdata"."jcgkzx_permission" (username, module) VALUES (%s, %s)',
        (user, '精神障碍')
    )

conn.commit()
conn.close()
```

## 数据库表结构

### jcgkzx_permission 表

```sql
CREATE TABLE "ywdata"."jcgkzx_permission" (
    username VARCHAR(255) NOT NULL,  -- 用户名
    module VARCHAR(255) NOT NULL,    -- 模块名称
    PRIMARY KEY (username, module)
);
```

### 模块名称列表

系统中已有的模块名称：
- `警情` - 警情案件模块
- `巡防` - 巡防统计模块
- `治综` - 治综平台数据统计模块
- `后台` - 后台管理模块
- `未成年人` - 未成年人模块
- `工作日志督导` - 工作日志督导模块
- `精神障碍` - 精神障碍患者预警模块（新增）

## 注意事项

1. **模块名称必须完全匹配**: 权限表中的 `module` 字段值必须是 `精神障碍`（不能是其他变体）
2. **用户名区分大小写**: 确保用户名与登录时使用的用户名完全一致
3. **权限立即生效**: 添加权限后，用户重新登录即可看到新模块
4. **数据库连接**: 确保应用能够正常连接到数据库
5. **Schema配置**: 确认数据库配置中的schema为 `ywdata`

## 联系方式

如有问题，请联系系统管理员或开发团队。
