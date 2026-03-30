import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from gonggong.service import session_manager as session_manager_module


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code=200,
        json_data=None,
        text="",
        headers=None,
        url="http://68.253.2.107/zhksh/system/user/getInfo",
        content=None,
    ):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self.text = text
        self.headers = headers or {"Content-Type": "application/json"}
        self.url = url
        self.content = content if content is not None else text.encode("utf-8")
        self.apparent_encoding = "utf-8"

    def json(self):
        return self._json_data


class TestSessionManager(unittest.TestCase):
    def test_manager_init_is_lazy(self) -> None:
        with patch.object(session_manager_module.requests, "Session") as mock_session:
            manager = session_manager_module.SessionManager()

        self.assertIsNone(manager.session)
        mock_session.assert_not_called()

    def test_get_session_logs_in_on_first_access(self) -> None:
        fake_session = Mock()
        fake_session.post.return_value = _FakeResponse(
            status_code=200,
            json_data={"code": 200, "msg": "操作成功"},
            text='{"code":200,"msg":"操作成功"}',
        )

        with patch.object(session_manager_module.requests, "Session", return_value=fake_session):
            manager = session_manager_module.SessionManager()
            session = manager.get_session()

        self.assertIs(session, fake_session)
        fake_session.post.assert_called_once()
        fake_session.get.assert_not_called()

    def test_near_expiry_validation_uses_existing_session(self) -> None:
        existing_session = Mock()
        existing_session.get.return_value = _FakeResponse(status_code=200, json_data={"code": 200})

        with patch.object(session_manager_module.requests, "Session") as mock_session:
            manager = session_manager_module.SessionManager()
            manager.session = existing_session
            manager.last_login_time = datetime.now() - timedelta(hours=3, minutes=45)

            expired = manager._is_session_expired()

        self.assertFalse(expired)
        existing_session.get.assert_called_once_with(manager.validate_url, timeout=manager.validation_timeout)
        mock_session.assert_not_called()


if __name__ == "__main__":
    unittest.main()
