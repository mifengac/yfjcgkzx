import io
import unittest
from datetime import datetime as real_datetime
from unittest.mock import patch

from flask import Flask
from openpyxl import load_workbook

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service import gambling_topic_service as service


class TestGamblingTopicService(unittest.TestCase):
    def test_resolve_gambling_topic_tags_uses_target_parent(self) -> None:
        tree_nodes = [
            {"id": "1", "pId": "OTHER", "name": "其他", "tag": "999"},
            {"id": "2", "pId": service.GAMBLING_TOPIC_PARENT_ID, "name": "赌博", "tag": "0301"},
            {"id": "3", "pId": service.GAMBLING_TOPIC_PARENT_ID, "name": "赌博", "tag": "0301"},
            {"id": "4", "pId": service.GAMBLING_TOPIC_PARENT_ID, "name": "开设赌场", "tag": "0302"},
        ]

        tag_csv, name_csv = service.resolve_gambling_topic_tags(tree_nodes)

        self.assertEqual(tag_csv, "0301,0302")
        self.assertEqual(name_csv, "赌博,开设赌场")

    def test_summarize_gambling_way_by_region_counts_keyword_categories(self) -> None:
        rows = [
            {"cmdName": "云城", "caseContents": "有人聚众打麻将赌博", "replies": ""},
            {"cmdName": "云城", "caseContents": "现场有人玩牛牛和炸金花", "replies": ""},
            {"cmdName": "罗定", "caseContents": "群众举报网络赌博平台", "replies": ""},
        ]

        result = service.summarize_gambling_way_by_region(rows)

        self.assertEqual(result["rows"][0]["cmdName"], "云城")
        self.assertEqual(result["rows"][0]["counts"]["麻将"], 1)
        self.assertEqual(result["rows"][0]["counts"]["斗牛"], 1)
        self.assertEqual(result["rows"][0]["counts"]["扑克"], 1)
        self.assertEqual(result["rows"][0]["total"], 3)
        self.assertEqual(rows[1]["gamblingWayLabels"], "斗牛、扑克")
        self.assertIn("炸金花", rows[1]["gamblingWayKeywords"])

    def test_summarize_wilderness_by_region_uses_reply_keywords(self) -> None:
        rows = [
            {"cmdName": "云城", "caseContents": "赌博", "replies": "民警到山腰树林处查处"},
            {"cmdName": "罗定", "caseContents": "赌博", "replies": "室内麻将档"},
            {"cmdName": "云城", "caseContents": "赌博", "replies": "山脚野外有人聚赌"},
        ]

        result = service.summarize_wilderness_by_region(rows)

        self.assertEqual(result["rows"], [{"cmdName": "云城", "total": 2}])
        self.assertEqual(len(result["details"]), 2)
        self.assertIn("山腰", rows[0]["gamblingWildernessKeywords"])
        self.assertEqual(rows[1]["gamblingWildernessKeywords"], "")

    def test_run_analysis_returns_selected_custom_dimensions(self) -> None:
        params = {
            "beginDate": "2026-04-01 00:00:00",
            "endDate": "2026-04-02 00:00:00",
            "m2mStartTime": "2026-03-31 00:00:00",
            "m2mEndTime": "2026-04-01 00:00:00",
        }
        rows = [
            {
                "cmdName": "云城",
                "caseContents": "出租屋有人打麻将赌博",
                "replies": "处警发现山脚树林有人聚赌",
                "callTime": "2026-04-01 01:00:00",
            }
        ]

        with patch.object(service, "resolve_gambling_topic_tags", return_value=("0301", "赌博")), patch.object(
            service,
            "fetch_all_case_list",
            return_value=rows,
        ) as mock_fetch:
            results, _base, all_data, _options, _meta = service.run_gambling_topic_analysis(
                params,
                ["gambling_way", "wilderness"],
            )

        self.assertIn("gambling_way", results)
        self.assertIn("wilderness", results)
        self.assertEqual(results["gambling_way"]["rows"][0]["counts"]["麻将"], 1)
        self.assertEqual(results["wilderness"]["rows"][0]["total"], 1)
        self.assertEqual(all_data[0]["gamblingWayLabels"], "麻将")
        self.assertEqual(mock_fetch.call_args.kwargs["max_page_size"], service.GAMBLING_TOPIC_UPSTREAM_PAGE_SIZE)

    def test_generate_excel_adds_custom_dimension_sheets(self) -> None:
        all_data = [
            {
                "caseNo": "JQ001",
                "callTime": "2026-04-01 01:00:00",
                "cmdName": "云城",
                "cmdId": "445302000000",
                "dutyDeptName": "测试所",
                "caseContents": "有人打麻将赌博",
                "replies": "处警发现山腰树林有人聚赌",
                "gamblingWayLabels": "麻将",
                "gamblingWayKeywords": "麻将",
                "gamblingWildernessKeywords": "山腰、树林",
            }
        ]
        analysis_results = {
            "gambling_way": {
                "columns": ["麻将"],
                "rows": [{"cmdName": "云城", "counts": {"麻将": 1}, "total": 1}],
                "details": all_data,
            },
            "wilderness": {
                "rows": [{"cmdName": "云城", "total": 1}],
                "details": all_data,
            },
        }

        buffer = service.generate_gambling_topic_excel(
            analysis_results,
            all_data,
            ["gambling_way", "wilderness"],
            begin_date="2026-04-01 00:00:00",
            end_date="2026-04-02 00:00:00",
        )
        workbook = load_workbook(io.BytesIO(buffer.getvalue()))

        self.assertEqual(workbook.sheetnames, ["赌博专题", "赌博方式", "涉山林野外赌博"])
        way_sheet = workbook["赌博方式"]
        wild_sheet = workbook["涉山林野外赌博"]
        self.assertEqual(way_sheet["A1"].value, "赌博方式")
        self.assertIn("命中关键词", [cell.value for row in way_sheet.iter_rows() for cell in row])
        self.assertEqual(wild_sheet["A1"].value, "涉山林野外赌博")
        self.assertIn("山腰、树林", [cell.value for row in wild_sheet.iter_rows() for cell in row])

    def test_build_export_filename_matches_expected_format(self) -> None:
        filename = service.build_export_filename(
            "2026-04-01 00:00:00",
            "2026-04-02 00:00:00",
            now=real_datetime(2026, 4, 20, 8, 9, 10),
        )
        self.assertEqual(filename, "2026-04-01-2026-04-02赌博专题警情分析20260420080910.xlsx")


class TestGamblingTopicRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jingqing_fenxi_bp, url_prefix="/jingqing_fenxi")
        self.client = app.test_client()

    def test_analyze_route_returns_service_result(self) -> None:
        with patch(
            "jingqing_fenxi.routes.gambling_topic_routes.run_gambling_topic_analysis",
            return_value=({"gambling_way": {"rows": []}}, {}, [], {}, {}),
        ) as mock_run:
            response = self.client.post(
                "/jingqing_fenxi/api/gambling-topic/analyze",
                data={"dimensions[]": ["gambling_way"]},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["code"], 0)
        self.assertEqual(mock_run.call_args.args[1], ["gambling_way"])

    def test_download_route_returns_workbook(self) -> None:
        with patch(
            "jingqing_fenxi.routes.gambling_topic_routes.run_gambling_topic_analysis",
            return_value=({}, {}, [], {}, {"beginDate": "2026-04-01 00:00:00", "endDate": "2026-04-02 00:00:00"}),
        ), patch(
            "jingqing_fenxi.routes.gambling_topic_routes.generate_gambling_topic_excel",
            return_value=io.BytesIO(b"test-export"),
        ), patch(
            "jingqing_fenxi.routes.gambling_topic_routes.build_export_filename",
            return_value="gambling.xlsx",
        ):
            response = self.client.get("/jingqing_fenxi/download/gambling-topic?dimensions[]=gambling_way")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"test-export")
        self.assertIn("gambling.xlsx", response.headers["Content-Disposition"])


if __name__ == "__main__":
    unittest.main()
