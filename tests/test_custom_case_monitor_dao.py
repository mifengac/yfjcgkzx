import unittest
from unittest.mock import patch

from jingqing_fenxi.dao import custom_case_monitor_dao as dao


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.executed = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((" ".join(str(sql).split()), params))

    def fetchone(self):
        return self.fetchone_results.pop(0) if self.fetchone_results else None

    def fetchall(self):
        return self.fetchall_results.pop(0) if self.fetchall_results else []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.committed = False

    def close(self):
        self.closed = True


class TestCustomCaseMonitorDao(unittest.TestCase):
    def test_list_schemes_attaches_rules(self) -> None:
        cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "id": 1,
                        "scheme_name": "流浪/乞讨警情",
                        "scheme_code": "wander_begging",
                        "description": "",
                        "is_enabled": True,
                        "created_at": "2026-04-05 00:00:00",
                        "updated_at": "2026-04-05 00:00:00",
                    }
                ],
                [
                    {
                        "id": 11,
                        "scheme_id": 1,
                        "field_name": "combined_text",
                        "operator": "contains_any",
                        "rule_values": ["流浪", "乞讨"],
                        "sort_order": 1,
                        "is_enabled": True,
                        "created_at": "2026-04-05 00:00:00",
                        "updated_at": "2026-04-05 00:00:00",
                    }
                ],
            ]
        )
        connection = _FakeConnection(cursor)

        with patch.object(dao, "get_database_connection", return_value=connection):
            rows = dao.list_schemes(include_disabled=True)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rules"][0]["field_name"], "combined_text")
        self.assertTrue(connection.closed)

    def test_create_scheme_commits_and_returns_scheme(self) -> None:
        cursor = _FakeCursor(fetchone_results=[{"id": 9}])
        connection = _FakeConnection(cursor)
        created_scheme = {"id": 9, "scheme_name": "出租屋警情", "rules": []}

        with patch.object(dao, "get_database_connection", return_value=connection), patch.object(
            dao,
            "get_scheme_by_id",
            return_value=created_scheme,
        ):
            scheme = dao.create_scheme(
                scheme_name="出租屋警情",
                scheme_code="rental_house",
                description="test",
                is_enabled=True,
                rules=[
                    {
                        "field_name": "combined_text",
                        "operator": "contains_any",
                        "rule_values": ["出租屋", "租赁"],
                        "sort_order": 1,
                        "is_enabled": True,
                    }
                ],
            )

        self.assertTrue(connection.committed)
        self.assertEqual(scheme["id"], 9)
        self.assertTrue(any("INSERT INTO" in sql for sql, _ in cursor.executed))

    def test_list_schemes_reads_group_no_when_column_exists(self) -> None:
        cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "id": 1,
                        "scheme_name": "test",
                        "scheme_code": "test_scheme",
                        "description": "",
                        "is_enabled": True,
                        "created_at": "2026-04-05 00:00:00",
                        "updated_at": "2026-04-05 00:00:00",
                    }
                ],
                [
                    {
                        "id": 11,
                        "scheme_id": 1,
                        "field_name": "combined_text",
                        "operator": "contains_any",
                        "rule_values": ["knife"],
                        "group_no": 2,
                        "sort_order": 1,
                        "is_enabled": True,
                        "created_at": "2026-04-05 00:00:00",
                        "updated_at": "2026-04-05 00:00:00",
                    }
                ],
            ]
        )
        connection = _FakeConnection(cursor)

        with patch.object(dao, "get_database_connection", return_value=connection), patch.object(
            dao,
            "_rule_group_column_exists",
            return_value=True,
        ):
            rows = dao.list_schemes(include_disabled=True)

        self.assertEqual(rows[0]["rules"][0]["group_no"], 2)

    def test_create_scheme_writes_group_no_when_column_exists(self) -> None:
        cursor = _FakeCursor(fetchone_results=[{"id": 9}])
        connection = _FakeConnection(cursor)
        created_scheme = {"id": 9, "scheme_name": "test", "rules": []}

        with patch.object(dao, "get_database_connection", return_value=connection), patch.object(
            dao,
            "get_scheme_by_id",
            return_value=created_scheme,
        ), patch.object(dao, "_rule_group_column_exists", return_value=True):
            dao.create_scheme(
                scheme_name="test",
                scheme_code="test_scheme",
                description="test",
                is_enabled=True,
                rules=[
                    {
                        "field_name": "combined_text",
                        "operator": "contains_any",
                        "rule_values": ["knife"],
                        "group_no": 3,
                        "sort_order": 1,
                        "is_enabled": True,
                    }
                ],
            )

        rule_insert_calls = [
            (sql, params)
            for sql, params in cursor.executed
            if "custom_case_monitor_rule" in sql and "INSERT INTO" in sql
        ]
        self.assertTrue(rule_insert_calls)
        self.assertIn("group_no", rule_insert_calls[0][0])
        self.assertEqual(rule_insert_calls[0][1][4], 3)


if __name__ == "__main__":
    unittest.main()
