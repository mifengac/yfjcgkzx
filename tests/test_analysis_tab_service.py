import unittest
from unittest.mock import Mock, patch

from werkzeug.datastructures import MultiDict

from jingqing_fenxi.service import analysis_tab_service as service


class TestAnalysisTabService(unittest.TestCase):
    def test_get_nature_tree_falls_back_when_api_client_singleton_is_stale(self):
        stale_client = Mock(spec=["request_with_retry"])
        stale_response = Mock(status_code=200)
        stale_response.json.return_value = [{"id": "01", "name": "警情性质"}]
        stale_client.request_with_retry.return_value = stale_response

        with patch("jingqing_fenxi.service.analysis_tab_service.api_client", stale_client):
            data = service.get_nature_tree_new_view_data()

        self.assertEqual(data, [{"id": "01", "name": "警情性质"}])
        stale_client.request_with_retry.assert_called_once_with(
            "GET",
            "/dsjfx/nature/treeNewViewData",
            timeout=15,
        )

    def test_build_srr_payload_uses_submitted_nature_codes_without_plan_lookup(self):
        form = MultiDict(
            [
                ("caseTypeSource", "nature"),
                ("caseTypeIds[]", "01"),
                ("newOriCharaSubclassNo", "01,0101"),
                ("newOriCharaSubclass", "纠纷警情,打架斗殴"),
                ("beginDate", "2026-04-01 00:00:00"),
                ("endDate", "2026-04-02 00:00:00"),
            ]
        )

        with patch(
            "jingqing_fenxi.service.analysis_tab_service.get_tree_view_data",
            return_value=[{"id": "01", "pId": "root", "tag": "SHOULD_NOT_USE"}],
        ) as mock_get_tree:
            payload = service._build_srr_payload(form)

        self.assertEqual(payload["charaNo"], "01,0101")
        self.assertEqual(payload["chara"], "纠纷警情,打架斗殴")
        mock_get_tree.assert_not_called()

    def test_build_srr_payload_keeps_plan_tag_lookup_for_plan_source(self):
        form = MultiDict(
            [
                ("caseTypeSource", "plan"),
                ("caseTypeIds[]", "p1"),
                ("newOriCharaSubclassNo", "fallback"),
                ("newOriCharaSubclass", "打架斗殴"),
                ("beginDate", "2026-04-01 00:00:00"),
                ("endDate", "2026-04-02 00:00:00"),
            ]
        )

        with patch(
            "jingqing_fenxi.service.analysis_tab_service.get_tree_view_data",
            return_value=[
                {"id": "p1", "name": "预案父项"},
                {"id": "c1", "pId": "p1", "tag": "T001", "name": "子项1"},
                {"id": "c2", "pId": "p1", "tag": "T002", "name": "子项2"},
            ],
        ):
            payload = service._build_srr_payload(form)

        self.assertEqual(payload["charaNo"], "T001,T002")


if __name__ == "__main__":
    unittest.main()
