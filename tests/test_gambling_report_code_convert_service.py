import io
import unittest
from datetime import datetime as real_datetime
from unittest.mock import patch

from docx import Document
from flask import Flask

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service import gambling_report_code_convert_service as service


class TestGamblingReportCodeConvertService(unittest.TestCase):
    def test_extract_station_codes_returns_unique_12_digit_codes(self) -> None:
        text = "445321510000、445321510000、445381650000，2026年1024起"

        self.assertEqual(service.extract_station_codes(text), ["445321510000", "445381650000"])

    def test_convert_markdown_station_codes_to_docx_replaces_known_codes(self) -> None:
        markdown = "# 赌博分析报告\n\n一、数据分析\n\n445321510000的89起，445381650000的57起。"
        with patch.object(
            service,
            "fetch_station_name_map",
            return_value={"445321510000": "测试派出所"},
        ) as mock_fetch:
            output = service.convert_markdown_station_codes_to_docx(markdown.encode("utf-8"), "report.md")

        mock_fetch.assert_called_once_with(["445321510000", "445381650000"])
        document = Document(io.BytesIO(output.getvalue()))
        text = "\n".join(paragraph.text for paragraph in document.paragraphs)
        self.assertIn("测试派出所的89起", text)
        self.assertIn("445381650000", text)
        self.assertIn("未转换派出所代码", text)

    def test_build_code_convert_filename_uses_original_stem(self) -> None:
        filename = service.build_code_convert_filename(
            "赌博警情分析报告.md",
            now=real_datetime(2026, 4, 28, 18, 1, 2),
        )

        self.assertEqual(filename, "赌博警情分析报告_派出所名称转换20260428180102.docx")


class TestGamblingReportCodeConvertRoute(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jingqing_fenxi_bp, url_prefix="/jingqing_fenxi")
        self.client = app.test_client()

    def test_code_convert_route_downloads_docx(self) -> None:
        with patch(
            "jingqing_fenxi.routes.gambling_topic_routes.convert_markdown_station_codes_to_docx",
            return_value=io.BytesIO(b"docx-data"),
        ) as mock_convert, patch(
            "jingqing_fenxi.routes.gambling_topic_routes.build_code_convert_filename",
            return_value="converted.docx",
        ):
            response = self.client.post(
                "/jingqing_fenxi/download/gambling-topic/code-convert",
                data={"file": (io.BytesIO("# 报告\n445321510000".encode("utf-8")), "report.md")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"docx-data")
        self.assertIn("converted.docx", response.headers["Content-Disposition"])
        self.assertEqual(mock_convert.call_args.args[1], "report.md")

    def test_code_convert_route_rejects_non_markdown_file(self) -> None:
        response = self.client.post(
            "/jingqing_fenxi/download/gambling-topic/code-convert",
            data={"file": (io.BytesIO(b"content"), "report.txt")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("只支持上传 .md", response.get_json()["message"])


if __name__ == "__main__":
    unittest.main()
