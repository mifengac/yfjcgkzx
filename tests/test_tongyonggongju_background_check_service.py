from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from tongyonggongju.service import background_check_service as service


def _xlsx_bytes(headers, rows) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(headers)
    for row in rows:
        worksheet.append(row)
    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


class TestBackgroundCheckService(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.upload_dir_patch = patch.object(service, "UPLOAD_DIR", Path(self.tmp.name))
        self.upload_dir_patch.start()

    def tearDown(self) -> None:
        self.upload_dir_patch.stop()
        self.tmp.cleanup()

    def test_inspect_and_extract_id_numbers(self) -> None:
        file_bytes = _xlsx_bytes(
            ["姓名", "身份证号"],
            [
                ["张三", "441111199001011234"],
                ["李四", "441111199001011234"],
                ["王五", "invalid"],
                ["赵六", ""],
            ],
        )

        payload = service.inspect_and_store_workbook(file_bytes, "名单.xlsx")
        extracted = service.extract_id_numbers(payload["token"], 2)

        self.assertEqual(payload["columns"][1]["display"], "B - 身份证号")
        self.assertEqual(extracted["valid_count"], 2)
        self.assertEqual(extracted["unique_count"], 1)
        self.assertEqual(extracted["duplicate_count"], 1)
        self.assertEqual(extracted["invalid_count"], 1)
        self.assertEqual(extracted["people"][0]["id_number"], "441111199001011234")

    def test_run_background_check_builds_overview(self) -> None:
        file_bytes = _xlsx_bytes(
            ["身份证号"],
            [
                ["441111199001011234"],
                ["442222199002022345"],
            ],
        )
        payload = service.inspect_and_store_workbook(file_bytes, "名单.xlsx")

        with (
            patch.object(
                service,
                "query_prior_case_rows",
                return_value=[
                    {
                        "身份证号": "441111199001011234",
                        "案件编号": "A1",
                        "案件类型": "刑事",
                        "案件名称": "测试案件",
                        "立案时间": "2026-04-01 00:00:00",
                        "办案单位": "测试单位",
                    }
                ],
            ),
            patch.object(
                service,
                "query_dispute_rows",
                return_value=[{"身份证号": "442222199002022345", "姓名": "李四", "管理状态": "撤管"}],
            ),
            patch.object(
                service,
                "query_mental_health_rows",
                return_value=[{"身份证号": "441111199001011234", "姓名": "张三", "管理状态": "在管"}],
            ),
        ):
            result = service.run_background_check(payload["token"], 1)

        self.assertEqual(result["stats"]["去重后人数"], 2)
        self.assertEqual(result["stats"]["命中人数"], 2)
        self.assertEqual(result["overview"][0]["命中类型"], "前科、精神障碍")
        self.assertEqual(result["overview"][1]["矛盾纠纷状态"], "撤管")


if __name__ == "__main__":
    unittest.main()
