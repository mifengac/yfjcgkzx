import unittest

from hqzcsj.service.wcnr_1393_rate_utils import calc_rate_stats_bundle, calc_songjiao_stats


class TestWcnr1393SongjiaoStats(unittest.TestCase):
    def test_denominator_uses_only_fuhesongsheng_yes(self) -> None:
        rows = [
            {"地区": "445302", "是否送校": "是", "是否符合送生": "否"},
            {"地区": "445302", "是否送校": "否", "是否符合送生": "是"},
            {"地区": "445303", "是否送校": "是", "是否符合送生": "是"},
            {"地区": "445303", "是否送校": "是", "是否符合送生": "否"},
        ]

        num_by, denom_by, num_total, denom_total = calc_songjiao_stats(rows)

        self.assertEqual(num_by, {"445302": 1, "445303": 2})
        self.assertEqual(denom_by, {"445302": 1, "445303": 1})
        self.assertEqual(num_total, 3)
        self.assertEqual(denom_total, 2)

    def test_zljqjh_denominator_uses_wfzf(self) -> None:
        rows = [
            {"地区": "445302", "是否开具家庭教育指导书": "是"},
            {"地区": "445302", "是否开具家庭教育指导书": "否"},
            {"地区": "445303", "是否开具家庭教育指导书": "是"},
        ]
        wfzf_by = {"445302": 10, "445303": 5}
        wfzf_total = 15

        stats = calc_rate_stats_bundle(rows, wfzf_by=wfzf_by, wfzf_total=wfzf_total)
        num_by, denom_by, num_total, denom_total = stats["zljqjh_rate"]

        self.assertEqual(num_by, {"445302": 1, "445303": 1})
        self.assertEqual(denom_by, {"445302": 10, "445303": 5})
        self.assertEqual(num_total, 2)
        self.assertEqual(denom_total, 15)


if __name__ == "__main__":
    unittest.main()
