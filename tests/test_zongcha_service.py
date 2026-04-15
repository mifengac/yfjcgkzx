import unittest
from unittest.mock import patch

from hqzcsj.service import zongcha_service as service


class TestZongchaService(unittest.TestCase):
    def test_build_headers_preserves_base_headers_and_injects_auth(self) -> None:
        headers = service._build_headers(  # noqa: SLF001
            {"Accept": "application/json", "Existing": "1"},
            cookie="abc=1",
            authorization="Bearer token",
        )

        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Existing"], "1")
        self.assertEqual(headers["Cookie"], "abc=1")
        self.assertEqual(headers["Authorization"], "Bearer token")

    def test_filter_jobs_accepts_registry_key_and_name(self) -> None:
        jobs = [
            service.ZongchaJobDef("训诫书", "t1", ["id"], {}, ["xjs_tfsj"]),
            service.ZongchaJobDef("案件信息", "t2", ["id"], {}, ["ajxx_lasj"]),
        ]

        filtered = service._filter_jobs(jobs, sources=["xjs", "案件信息"])  # noqa: SLF001

        self.assertEqual([job.name for job in filtered], ["训诫书", "案件信息"])

    def test_split_time_windows_returns_single_range_when_no_time_fields(self) -> None:
        job = service.ZongchaJobDef("测试", "demo", ["id"], {"pageSize": "100"}, [])

        windows = service._split_time_windows_if_needed(  # noqa: SLF001
            job=job,
            start_time="2026-04-01 00:00:00",
            end_time="2026-04-08 00:00:00",
            url="http://example.com",
            headers={},
            base_form={"pageSize": "100", "json": "{}"},
        )

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0][0], service._parse_dt("2026-04-01 00:00:00"))  # noqa: SLF001
        self.assertEqual(windows[0][1], service._parse_dt("2026-04-08 00:00:00"))  # noqa: SLF001

    def test_split_time_windows_splits_when_cap_is_detected(self) -> None:
        job = service.ZongchaJobDef("测试", "demo", ["id"], {"pageSize": "100", "json": "{}"}, ["lrsj"])

        def fake_fetch_all_pages(*, base_form, **_kwargs):
            json_text = base_form.get("json") or ""
            if "2026-04-01 00:00:00" in json_text and "2026-04-08 00:00:00" in json_text:
                return ([{"id": "1"}] * 5000, True)
            return ([{"id": "1"}], False)

        with patch("hqzcsj.service.zongcha_service._fetch_all_pages", side_effect=fake_fetch_all_pages), patch(
            "hqzcsj.service.zongcha_service.os.getenv",
            side_effect=lambda key, default=None: {"ZFBA_RESULT_CAP": "5000", "ZFBA_SPLIT_MIN_SECONDS": "600"}.get(key, default),
        ):
            windows = service._split_time_windows_if_needed(  # noqa: SLF001
                job=job,
                start_time="2026-04-01 00:00:00",
                end_time="2026-04-08 00:00:00",
                url="http://example.com",
                headers={},
                base_form={
                    "pageSize": "100",
                    "json": '{"paramArray":[{"conditions":[{"fieldCode":"lrsj","operateSign":"10","values":[]}]}]}',
                },
            )

        self.assertGreater(len(windows), 1)
        self.assertLess(windows[0][0], windows[0][1])

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
