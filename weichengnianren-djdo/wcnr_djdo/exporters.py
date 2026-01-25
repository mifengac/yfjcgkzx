from __future__ import annotations

import io
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, List

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from .service import MetricResult


def _safe_filename_part(val: str) -> str:
    s = str(val or "").strip()
    return s.replace(":", "-").replace("/", "-").replace("\\", "-").replace(" ", "_")


def _excel_value(val: Any) -> Any:
    """
    openpyxl 单元格不支持 dict/list 等复杂类型，这里统一做可写入转换。
    """
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool, datetime)):
        return val
    if isinstance(val, Decimal):
        try:
            return float(val)
        except Exception:
            return str(val)
    if isinstance(val, (dict, list, tuple)):
        try:
            return json.dumps(val, ensure_ascii=False, default=str)
        except Exception:
            return str(val)
    # psycopg2 jsonb 可能是 list/dict；其余对象兜底为字符串
    return str(val)


def build_metric_xlsx(result: MetricResult, start_time: datetime, end_time: datetime) -> tuple[bytes, str]:
    wb = Workbook()
    ws = wb.active
    ws.title = result.title[:31]

    ws.append([result.title])
    ws.append([f"{start_time:%Y-%m-%d %H:%M:%S} - {end_time:%Y-%m-%d %H:%M:%S}"])
    ws.append([])

    ws.append(["柱状图数据"])
    ws.append(["地区"] + result.series)
    for r in result.chart_rows:
        ws.append([_excel_value(r.get("地区"))] + [_excel_value(r.get(s)) for s in result.series])
    ws.append([])

    ws.append(["明细表"])
    if result.detail_rows:
        cols = list(result.detail_rows[0].keys())
        ws.append(cols)
        for row in result.detail_rows:
            ws.append([_excel_value(row.get(c)) for c in cols])
        for idx, col in enumerate(cols, start=1):
            ws.column_dimensions[get_column_letter(idx)].width = min(40, max(10, len(str(col)) + 2))

    buf = io.BytesIO()
    wb.save(buf)
    ts = int(datetime.now().timestamp())
    filename = (
        f"{_safe_filename_part(start_time.strftime('%Y-%m-%d_%H-%M-%S'))}-"
        f"{_safe_filename_part(end_time.strftime('%Y-%m-%d_%H-%M-%S'))}{result.title}{ts}.xlsx"
    )
    return buf.getvalue(), filename


def build_details_xlsx(results: List[MetricResult], start_time: datetime, end_time: datetime) -> tuple[bytes, str]:
    wb = Workbook()
    wb.remove(wb.active)

    for result in results:
        ws = wb.create_sheet(title=result.title[:31])
        ws.append([result.title])
        ws.append([f"{start_time:%Y-%m-%d %H:%M:%S} - {end_time:%Y-%m-%d %H:%M:%S}"])
        ws.append([])

        if result.detail_rows:
            cols = list(result.detail_rows[0].keys())
            ws.append(cols)
            for row in result.detail_rows:
                ws.append([_excel_value(row.get(c)) for c in cols])
            for idx, col in enumerate(cols, start=1):
                ws.column_dimensions[get_column_letter(idx)].width = min(40, max(10, len(str(col)) + 2))

    buf = io.BytesIO()
    wb.save(buf)
    ts = int(datetime.now().timestamp())
    filename = (
        f"{_safe_filename_part(start_time.strftime('%Y-%m-%d_%H-%M-%S'))}-"
        f"{_safe_filename_part(end_time.strftime('%Y-%m-%d_%H-%M-%S'))}导出详情{ts}.xlsx"
    )
    return buf.getvalue(), filename


def build_overview_pdf(results: List[MetricResult], start_time: datetime, end_time: datetime) -> tuple[bytes, str]:
    try:
        from reportlab.lib import colors  # type: ignore
        from reportlab.lib.pagesizes import A4, landscape  # type: ignore
        from reportlab.lib.styles import getSampleStyleSheet  # type: ignore
        from reportlab.platypus import (  # type: ignore
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
        from reportlab.graphics.charts.barcharts import VerticalBarChart  # type: ignore
        from reportlab.graphics.charts.legends import Legend  # type: ignore
        from reportlab.graphics.shapes import Drawing  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"缺少依赖 reportlab，无法导出 PDF：{exc}") from exc

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=24,
        rightMargin=24,
        topMargin=24,
        bottomMargin=24,
    )
    styles = getSampleStyleSheet()
    story: List[Any] = []

    story.append(Paragraph("全市未成年人打架斗殴六项指标监测", styles["Title"]))
    story.append(Paragraph(f"{start_time:%Y-%m-%d %H:%M:%S} - {end_time:%Y-%m-%d %H:%M:%S}", styles["Normal"]))
    story.append(Spacer(1, 12))

    palette = [colors.HexColor("#60a5fa"), colors.HexColor("#34d399"), colors.HexColor("#f59e0b")]

    for idx, result in enumerate(results):
        story.append(Paragraph(result.title, styles["Heading2"]))

        drawing = Drawing(740, 260)
        chart = VerticalBarChart()
        chart.x = 40
        chart.y = 40
        chart.height = 180
        chart.width = 640
        chart.strokeColor = colors.black

        cats = [r["地区"] for r in result.chart_rows]
        data = []
        for series_name in result.series:
            data.append([float(r.get(series_name) or 0) for r in result.chart_rows])
        chart.data = data
        chart.categoryAxis.categoryNames = cats
        chart.categoryAxis.labels.boxAnchor = "ne"
        chart.categoryAxis.labels.angle = 45
        chart.valueAxis.valueMin = 0

        for i in range(len(result.series)):
            chart.bars[i].fillColor = palette[i % len(palette)]

        drawing.add(chart)
        legend = Legend()
        legend.x = 40
        legend.y = 230
        legend.colorNamePairs = [(palette[i % len(palette)], result.series[i]) for i in range(len(result.series))]
        drawing.add(legend)
        story.append(drawing)
        story.append(Spacer(1, 8))

        table_data = [["地区"] + result.series]
        for r in result.chart_rows:
            table_data.append([r.get("地区")] + [r.get(s) for s in result.series])
        t = Table(table_data, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(t)

        if idx != len(results) - 1:
            story.append(PageBreak())

    doc.build(story)
    filename = (
        f"{_safe_filename_part(start_time.strftime('%Y-%m-%d_%H-%M-%S'))}-"
        f"{_safe_filename_part(end_time.strftime('%Y-%m-%d_%H-%M-%S'))}全市未成年人打架斗殴六项指标监测.pdf"
    )
    return buf.getvalue(), filename
