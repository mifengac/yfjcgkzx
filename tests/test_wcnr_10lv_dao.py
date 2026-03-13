import unittest

from hqzcsj.dao.wcnr_10lv_dao import _is_zmjz_cover_num


AGE = "\u5e74\u9f84"
CASE_TYPE = "\u6848\u4ef6\u7c7b\u578b"
XINGJU = "\u662f\u5426\u5211\u62d8"
SECOND_SAME_AY = "2\u6b21\u8fdd\u6cd5\u4e14\u6848\u7531\u76f8\u540c\u4e14\u7b2c\u4e00\u6b21\u8fdd\u6cd5\u5f00\u5177\u4e86\u8bad\u8beb\u4e66"
THIRD_PLUS = "3\u6b21\u53ca\u4ee5\u4e0a\u8fdd\u6cd5"
ZHIJU_GT4 = "\u6cbb\u62d8\u5927\u4e8e4\u5929"
ZHUANMEN_APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8\u6559\u80b2\u7533\u8bf7\u4e66"


class TestWcnr10lvDao(unittest.TestCase):
    def test_zmjz_cover_num_requires_denominator_and_apply_flag(self) -> None:
        self.assertTrue(
            _is_zmjz_cover_num(
                {
                    AGE: "13",
                    CASE_TYPE: "\u5211\u4e8b",
                    XINGJU: "\u5426",
                    ZHUANMEN_APPLY: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_cover_num(
                {
                    AGE: "13",
                    CASE_TYPE: "\u5211\u4e8b",
                    XINGJU: "\u662f",
                    ZHUANMEN_APPLY: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_cover_num(
                {
                    AGE: "13",
                    CASE_TYPE: "\u884c\u653f",
                    SECOND_SAME_AY: "\u662f",
                    ZHUANMEN_APPLY: "\u5426",
                }
            )
        )

    def test_zmjz_cover_num_still_requires_age_gt_12(self) -> None:
        self.assertFalse(
            _is_zmjz_cover_num(
                {
                    AGE: "12",
                    CASE_TYPE: "\u884c\u653f",
                    THIRD_PLUS: "\u662f",
                    ZHUANMEN_APPLY: "\u662f",
                }
            )
        )

        self.assertTrue(
            _is_zmjz_cover_num(
                {
                    AGE: "13",
                    CASE_TYPE: "\u884c\u653f",
                    ZHIJU_GT4: "\u662f",
                    ZHUANMEN_APPLY: "\u662f",
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
