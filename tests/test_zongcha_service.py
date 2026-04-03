import unittest

from hqzcsj.service import zongcha_service as service


class TestZongchaService(unittest.TestCase):
    def test_wcnr_xyr_rows_fall_back_to_legacy_case_ref(self) -> None:
        rows = [
            {
                "ajxx_join_ajxx_ajbh": "",
                "ajxx_ajbhs": "AJ000",
                "xyrxx_sfzh": "440000199901011234",
                "xyrxx_lrsj": "2026-04-01 08:00:00",
            },
            {
                "ajxx_join_ajxx_ajbh": "AJ001",
                "xyrxx_sfzh": "440000199902021234",
                "xyrxx_lrsj": "2026-04-01 09:00:00",
            },
            {
                "ajxx_join_ajxx_ajbh": None,
                "ajxx_ajbhs": None,
                "xyrxx_sfzh": "440000199903031234",
                "xyrxx_lrsj": "2026-04-01 10:00:00",
            },
        ]

        filtered_rows, dropped_missing_case_ref = service._normalize_xyr_case_ref_rows(  # noqa: SLF001
            rows=rows,
            source_name="wcnr_xyr",
        )

        self.assertEqual(dropped_missing_case_ref, 1)
        self.assertEqual(len(filtered_rows), 2)
        self.assertEqual(filtered_rows[0]["ajxx_join_ajxx_ajbh"], "AJ000")
        self.assertEqual(filtered_rows[0]["ajxx_ajbhs"], "AJ000")
        self.assertEqual(filtered_rows[1]["ajxx_join_ajxx_ajbh"], "AJ001")

        final_rows, dropped_missing_pk = service._drop_rows_with_empty_pk(  # noqa: SLF001
            rows=filtered_rows,
            pk_fields=["ajxx_join_ajxx_ajbh", "xyrxx_sfzh"],
            source_name="wcnr_xyr",
        )

        self.assertEqual(dropped_missing_pk, 0)
        self.assertEqual(len(final_rows), 2)


if __name__ == "__main__":
    unittest.main()
