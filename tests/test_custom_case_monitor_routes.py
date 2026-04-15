import unittest
from unittest.mock import patch

from flask import Flask

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp


class TestCustomCaseMonitorRoutes(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(jingqing_fenxi_bp, url_prefix="/jingqing_fenxi")
        self.client = app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["username"] = "tester"

    def test_query_jobs_start_returns_job_id(self) -> None:
        self._login()
        with patch(
            "jingqing_fenxi.routes.custom_case_monitor_routes.start_query_custom_case_monitor_job",
            return_value="job-001",
        ) as mock_start:
            response = self.client.post(
                "/jingqing_fenxi/api/custom-case-monitor/query-jobs",
                json={
                    "scheme_id": 9,
                    "start_time": "2026-04-15 00:00:00",
                    "end_time": "2026-04-15 23:59:59",
                    "branches": ["云城"],
                    "page_num": 2,
                    "page_size": 30,
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["job_id"], "job-001")
        self.assertEqual(mock_start.call_args.kwargs["username"], "tester")
        self.assertEqual(mock_start.call_args.kwargs["page_num"], 2)

    def test_query_job_status_returns_404_when_missing(self) -> None:
        self._login()
        with patch(
            "jingqing_fenxi.routes.custom_case_monitor_routes.get_query_custom_case_monitor_job_status",
            return_value=None,
        ):
            response = self.client.get("/jingqing_fenxi/api/custom-case-monitor/query-jobs/missing-job")

        payload = response.get_json()
        self.assertEqual(response.status_code, 404)
        self.assertFalse(payload["success"])

    def test_query_job_status_returns_payload(self) -> None:
        self._login()
        fake_status = {
            "job_id": "job-001",
            "state": "success",
            "stage": "done",
            "message": "查询完成：命中 2 条",
            "progress": {"current": 2, "total": 2},
            "stats": {
                "upstream_row_count": 5,
                "rule_scanned_count": 5,
                "rule_match_count": 3,
                "branch_scanned_count": 3,
                "branch_filtered_count": 2,
            },
            "result": {"success": True, "total": 2, "rows": [{"caseNo": "A001"}]},
        }
        with patch(
            "jingqing_fenxi.routes.custom_case_monitor_routes.get_query_custom_case_monitor_job_status",
            return_value=fake_status,
        ):
            response = self.client.get("/jingqing_fenxi/api/custom-case-monitor/query-jobs/job-001")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["state"], "success")
        self.assertEqual(payload["data"]["stats"]["branch_filtered_count"], 2)

    def test_sync_query_route_keeps_existing_contract(self) -> None:
        fake_result = {
            "success": True,
            "scheme_name": "涉刀警情",
            "start_time": "2026-04-15 00:00:00",
            "end_time": "2026-04-15 23:59:59",
            "page_num": 1,
            "page_size": 15,
            "total": 1,
            "rows": [{"caseNo": "A001"}],
            "debug": {"upstream_row_count": 3, "rule_match_count": 1, "branch_filtered_count": 1},
        }
        with patch(
            "jingqing_fenxi.routes.custom_case_monitor_routes.query_custom_case_monitor_records",
            return_value=fake_result,
        ):
            response = self.client.post(
                "/jingqing_fenxi/api/custom-case-monitor/query",
                json={
                    "scheme_id": 1,
                    "start_time": "2026-04-15 00:00:00",
                    "end_time": "2026-04-15 23:59:59",
                    "branches": [],
                    "page_num": 1,
                    "page_size": 15,
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["total"], 1)


if __name__ == "__main__":
    unittest.main()
