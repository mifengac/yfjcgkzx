import unittest
from unittest.mock import Mock, patch

from gonggong.service import upstream_jingqing_client as client_module


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="", headers=None, url="http://example.com/data"):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self.text = text
        self.headers = headers or {"Content-Type": "application/json"}
        self.url = url

    def json(self):
        return self._json_data


class TestUpstreamJingqingClient(unittest.TestCase):
    def setUp(self) -> None:
        client_module.JingQingApiClient._instance = None

    def tearDown(self) -> None:
        client_module.JingQingApiClient._instance = None

    def test_client_init_is_lazy_and_does_not_login_immediately(self) -> None:
        fake_session = Mock()
        with patch.object(client_module.requests, "Session", return_value=fake_session):
            client = client_module.JingQingApiClient()

        self.assertIs(client.session, fake_session)
        fake_session.get.assert_not_called()
        fake_session.post.assert_not_called()
        self.assertFalse(client._logged_in)

    def test_request_with_retry_logs_in_before_first_request(self) -> None:
        fake_session = Mock()
        fake_session.get.return_value = _FakeResponse(status_code=200)
        with patch.object(client_module.requests, "Session", return_value=fake_session):
            client = client_module.JingQingApiClient()

        client.login = Mock(return_value=True)
        response = client.request_with_retry("GET", "/dsjfx/plan/treeViewData", timeout=15)

        self.assertEqual(response.status_code, 200)
        client.login.assert_called_once_with()
        fake_session.get.assert_called_once()

    def test_get_nature_tree_new_view_data_uses_nature_endpoint(self) -> None:
        fake_session = Mock()
        with patch.object(client_module.requests, "Session", return_value=fake_session):
            client = client_module.JingQingApiClient()

        with patch.object(
            client,
            "request_with_retry",
            return_value=_FakeResponse(json_data=[{"id": "01", "name": "测试"}]),
        ) as mock_request:
            data = client.get_nature_tree_new_view_data()

        self.assertEqual(data, [{"id": "01", "name": "测试"}])
        mock_request.assert_called_once_with("GET", "/dsjfx/nature/treeNewViewData", timeout=15)


if __name__ == "__main__":
    unittest.main()
