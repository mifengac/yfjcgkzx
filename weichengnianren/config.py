"""
未成年人模块前端列配置。

通过在此文件中配置列的 key 与显示名称，控制页面表格中展示哪些字段以及顺序。

说明：
- key 必须与查询结果中的列名一致（例如 SQL 中的 "警情编号"、"姓名" 等中文列名）；
- label 为前端表头显示文本；
- 特殊列：
  - "法律文书JSON列表" 将在模板中使用解析后的字段进行渲染，显示文书名称并可点击跳转。
"""

from __future__ import annotations

from typing import Dict, List

ColumnConfig = Dict[str, str]

COLUMN_DEFINITIONS: List[ColumnConfig] = [
    {"key": "警情编号", "label": "警情编号"},
    {"key": "报警时间", "label": "报警时间"},
    {"key": "原始警情性质", "label": "原始警情性质"},
    {"key": "警情标注", "label": "警情标注"},
    {"key": "管辖单位名称", "label": "管辖单位名称"},
    {"key": "警情地址", "label": "警情地址"},
    {"key": "报警内容", "label": "报警内容"},
    {"key": "案件编号", "label": "案件编号"},
    {"key": "案件名称", "label": "案件名称"},
    {"key": "简要案情", "label": "简要案情"},
    {"key": "办案单位名称", "label": "办案单位名称"},
    {"key": "立案日期", "label": "立案日期"},
    {"key": "案由", "label": "案由"},
    {"key": "案件类型", "label": "案件类型"},
    {"key": "案件状态名称", "label": "案件状态"},
    {"key": "姓名", "label": "姓名"},
    {"key": "性别", "label": "性别"},
    {"key": "年龄", "label": "年龄"},
    {"key": "证件号码", "label": "证件号码"},
    {"key": "户籍地", "label": "户籍地"},
    {"key": "现住址", "label": "现住址"},
    {"key": "角色名称", "label": "角色名称"},
    {"key": "法律文书JSON列表", "label": "法律文书"},
    {"key": "拘留天数", "label": "拘留天数"},
    {"key": "罚款金额", "label": "罚款金额"},
    {"key": "是否拘留不执行", "label": "是否拘留不执行"},
    {"key": "关联类型", "label": "关联类型"},
]

