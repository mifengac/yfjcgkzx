import shutil
import subprocess
import textwrap
import unittest


class TestAnalysisCaseTypeSourceJs(unittest.TestCase):
    @unittest.skipIf(shutil.which("node") is None, "node is not available")
    def test_case_type_source_helpers(self):
        script = textwrap.dedent(
            r"""
            const assert = require("assert");
            const helper = require("./jingqing_fenxi/static/js/analysis_case_type_source.js");

            const natureTree = [
              { id: "01", name: "X" },
              { id: "0101", pId: "01", name: "Y" },
              { id: "010101", pId: "0101", name: "Y-1" },
              { id: "02", name: "Z" }
            ];
            assert.deepStrictEqual(
              helper.getVisibleNodes(natureTree, "nature").map((item) => item.id),
              ["01", "02"]
            );
            assert.deepStrictEqual(
              helper.collectSelectionPayload(natureTree, ["01"], "nature"),
              { codes: ["01", "0101", "010101"], names: ["X", "Y", "Y-1"] }
            );

            const planTree = [
              { id: "p1", name: "预案父项" },
              { id: "c1", pId: "p1", tag: "T001", name: "子项1" },
              { id: "c2", pId: "p1", tag: "T002", name: "子项2" }
            ];
            assert.deepStrictEqual(
              helper.collectSelectionPayload(planTree, ["p1"], "plan"),
              { codes: ["T001", "T002"], names: ["子项1", "子项2"] }
            );
            """
        )

        subprocess.run(["node", "-e", script], check=True)


if __name__ == "__main__":
    unittest.main()
