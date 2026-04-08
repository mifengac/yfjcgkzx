from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence

from flask import Response, abort, jsonify, request, send_file
from openpyxl import Workbook


TableRows = Sequence[Mapping[str, Any]]


class SummaryDetailProvider(Protocol):
    def get_subject(self, subject_id: Any) -> Optional[Mapping[str, Any]]:
        ...

    def list_summary_rows(
        self,
        subject: Mapping[str, Any],
        *,
        region_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        ...

    def list_detail_rows(
        self,
        subject: Mapping[str, Any],
        region_code: str,
    ) -> List[Dict[str, Any]]:
        ...

    def list_all_rows(self, subject: Mapping[str, Any]) -> List[Dict[str, Any]]:
        ...

    def get_display_name(self, subject: Mapping[str, Any]) -> str:
        ...

    def get_rows_export_name(self, subject: Mapping[str, Any]) -> str:
        ...


def _build_headers(rows: TableRows, preserve_header_order: bool) -> List[str]:
    if not rows:
        return []
    if preserve_header_order:
        return list(rows[0].keys())
    return sorted({key for row in rows for key in row.keys()})


def download_rows_as_csv(
    rows: TableRows,
    filename: str,
    *,
    preserve_header_order: bool = False,
) -> Response:
    output = StringIO()
    if rows:
        headers = _build_headers(rows, preserve_header_order)
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: (row.get(key) or "") for key in headers})
    else:
        output.write("无数据\n")

    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


def download_rows_as_excel(
    rows: TableRows,
    filename: str,
    *,
    preserve_header_order: bool = False,
    sheet_title: str = "统计数据",
) -> Response:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_title

    if rows:
        headers = _build_headers(rows, preserve_header_order)
        sheet.append(headers)
        for row in rows:
            sheet.append([(row.get(key) or "") for key in headers])
    else:
        sheet.append(["无数据"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def download_rows(
    rows: TableRows,
    filename: str,
    export_format: str,
    *,
    preserve_header_order: bool = False,
    sheet_title: str = "统计数据",
) -> Response:
    if export_format == "excel":
        return download_rows_as_excel(
            rows,
            filename,
            preserve_header_order=preserve_header_order,
            sheet_title=sheet_title,
        )
    return download_rows_as_csv(rows, filename, preserve_header_order=preserve_header_order)


@dataclass(frozen=True)
class SummaryDetailController:
    provider: SummaryDetailProvider
    region_map: Mapping[str, str]
    subject_context_name: str = "subject"
    summary_context_name: str = "summary_rows"
    region_context_name: str = "region_map"
    region_param_name: str = "dwdm"
    detail_payload_key: str = "data"
    format_param_name: str = "format"

    def get_subject_or_404(self, subject_id: Any) -> Mapping[str, Any]:
        subject = self.provider.get_subject(subject_id)
        if not subject:
            abort(404)
        return subject

    def build_summary_page_context(self, subject_id: Any) -> Dict[str, Any]:
        subject = self.get_subject_or_404(subject_id)
        summary_rows = self.provider.list_summary_rows(subject)
        return {
            self.subject_context_name: subject,
            self.summary_context_name: summary_rows,
        }

    def build_rows_page_context(self, subject_id: Any) -> Dict[str, Any]:
        subject = self.get_subject_or_404(subject_id)
        region_code = self._get_region_code()

        if region_code:
            rows = self.provider.list_detail_rows(subject, region_code)
            title = f"{self.provider.get_display_name(subject)} - {self.get_region_name(region_code)}"
        else:
            rows = self.provider.list_all_rows(subject)
            title = self.provider.get_display_name(subject)

        return {
            "title": title,
            self.subject_context_name: subject,
            "rows": rows,
            self.region_param_name: region_code,
            self.region_context_name: self.region_map,
        }

    def json_detail(self, subject_id: Any) -> Response:
        subject = self.get_subject_or_404(subject_id)
        region_code = self._get_required_region_code()
        if isinstance(region_code, Response):
            return region_code

        try:
            rows = self.provider.list_detail_rows(subject, region_code)
        except ValueError as exc:
            return jsonify({"success": False, "message": str(exc)}), 400

        return jsonify({"success": True, self.detail_payload_key: rows or []})

    def download_detail(
        self,
        subject_id: Any,
        *,
        empty_message: str = "暂无可导出的数据",
        filename_suffix: str = "详情表",
    ) -> Response:
        subject = self.get_subject_or_404(subject_id)
        region_code = self._get_required_region_code()
        if isinstance(region_code, Response):
            return region_code

        export_format = self._get_export_format()

        try:
            rows = self.provider.list_detail_rows(subject, region_code)
        except ValueError as exc:
            return jsonify({"success": False, "message": str(exc)}), 400

        if not rows:
            return jsonify({"success": False, "message": empty_message}), 400

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        display_name = self.provider.get_display_name(subject)
        filename_prefix = f"{display_name}{timestamp}{filename_suffix}"
        extension = "xlsx" if export_format == "excel" else "csv"
        return download_rows(rows, f"{filename_prefix}.{extension}", export_format)

    def download_summary(self, subject_id: Any) -> Response:
        subject = self.get_subject_or_404(subject_id)
        export_format = self._get_export_format()
        region_code = self._get_region_code()

        try:
            rows = self.provider.list_summary_rows(subject, region_code=region_code)
        except ValueError as exc:
            return jsonify({"success": False, "message": str(exc)}), 400

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        display_name = self.provider.get_display_name(subject)
        extension = "xlsx" if export_format == "excel" else "csv"
        return download_rows(rows, f"{display_name}{timestamp}.{extension}", export_format)

    def download_rows_csv(self, subject_id: Any) -> Response:
        subject = self.get_subject_or_404(subject_id)
        region_code = self._get_region_code()

        if region_code:
            rows = self.provider.list_detail_rows(subject, region_code)
            filename = f"{self.get_region_name(region_code)}-{self.provider.get_rows_export_name(subject)}.csv"
        else:
            rows = self.provider.list_all_rows(subject)
            filename = f"{self.provider.get_rows_export_name(subject)}.csv"

        return download_rows_as_csv(rows or [], filename)

    def get_region_name(self, region_code: str) -> str:
        for name, code in self.region_map.items():
            if code == region_code:
                return name
        return region_code

    def _get_region_code(self) -> Optional[str]:
        region_code = (request.args.get(self.region_param_name) or "").strip()
        return region_code or None

    def _get_required_region_code(self) -> str | Response:
        region_code = self._get_region_code()
        if region_code:
            return region_code
        return jsonify({"success": False, "message": f"缺少参数 {self.region_param_name}"}), 400

    def _get_export_format(self) -> str:
        return (request.args.get(self.format_param_name) or "csv").strip().lower()
