from __future__ import annotations

import csv
from dataclasses import dataclass
from io import BytesIO, StringIO
from typing import Any, Iterable, Mapping, Sequence, Tuple

from openpyxl import Workbook

from zidingyi_baobiao.core.exceptions import ValidationError


@dataclass(frozen=True)
class ExportColumn:
    key: str
    label: str


def export_csv(rows: Sequence[Mapping[str, Any]], columns: Sequence[ExportColumn]) -> bytes:
    """
    导出 CSV（标准库）。
    """
    if not columns:
        raise ValidationError("columns 不能为空")

    sio = StringIO(newline="")
    writer = csv.writer(sio)
    writer.writerow([c.label for c in columns])
    for row in rows:
        writer.writerow([_stringify_csv_cell(row.get(c.key)) for c in columns])
    return sio.getvalue().encode("utf-8-sig")  # Excel 友好（带 BOM）


def export_xlsx(rows: Sequence[Mapping[str, Any]], columns: Sequence[ExportColumn]) -> bytes:
    """
    导出 XLSX（openpyxl）。
    """
    if not columns:
        raise ValidationError("columns 不能为空")

    wb = Workbook()
    ws = wb.active
    ws.title = "export"

    ws.append([c.label for c in columns])
    for row in rows:
        ws.append([row.get(c.key) for c in columns])

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def build_export_columns(column_defs: Iterable[Tuple[str, str]]) -> Sequence[ExportColumn]:
    """
    将 (key, label) 列定义转为 ExportColumn 列表。
    """
    cols: list[ExportColumn] = []
    for key, label in column_defs:
        key = str(key or "").strip()
        label = str(label or "").strip() or key
        if not key:
            raise ValidationError("列 key 不能为空")
        cols.append(ExportColumn(key=key, label=label))
    return cols


def _stringify_csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return str(value)
    return str(value)

