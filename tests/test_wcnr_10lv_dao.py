import unittest

from hqzcsj.dao.wcnr_10lv_dao import (
    _is_zmjz_ratio_den_row,
    _is_zmjz_ratio_num_row,
)


QUALIFIED = "\u662f\u5426\u7b26\u5408\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2"
APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8(\u77eb\u6cbb)\u6559\u80b2\u7533\u8bf7\u4e66"
LEGACY_APPLY = "\u662f\u5426\u5f00\u5177\u4e13\u95e8\u6559\u80b2\u7533\u8bf7\u4e66"


class TestWcnr10lvDao(unittest.TestCase):
    def test_zmjz_ratio_den_requires_qualified_flag(self) -> None:
        self.assertTrue(
            _is_zmjz_ratio_den_row(
                {
                    QUALIFIED: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_ratio_den_row(
                {
                    QUALIFIED: "\u5426",
                }
            )
        )

    def test_zmjz_ratio_num_requires_qualified_and_apply_flags(self) -> None:
        self.assertFalse(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u5426",
                    APPLY: "\u662f",
                }
            )
        )

        self.assertTrue(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    APPLY: "\u662f",
                }
            )
        )

        self.assertFalse(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    APPLY: "\u5426",
                }
            )
        )

    def test_zmjz_ratio_num_accepts_legacy_apply_field_name(self) -> None:
        self.assertTrue(
            _is_zmjz_ratio_num_row(
                {
                    QUALIFIED: "\u662f",
                    LEGACY_APPLY: "\u662f",
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
