from __future__ import annotations

from typing import Sequence

from jingqing_fenxi.service.special_case_tab_service import (
    build_defaults_payload,
    export_special_case_records,
    query_special_case_records,
)


CASE_TYPE = 'rental'
CASE_LABEL = '出租屋警情'
CASE_KEYWORDS = ['出租屋', '租赁']


def defaults_payload():
    return build_defaults_payload(CASE_TYPE, CASE_LABEL)


def query_rental_case_records(start_time: str, end_time: str, branches: Sequence[str] | None, page_num: int, page_size: int):
    return query_special_case_records(CASE_TYPE, CASE_LABEL, CASE_KEYWORDS, start_time, end_time, branches, page_num, page_size)


def export_rental_case_records(export_format: str, start_time: str, end_time: str, branches: Sequence[str] | None):
    return export_special_case_records(CASE_TYPE, CASE_LABEL, CASE_KEYWORDS, export_format, start_time, end_time, branches)