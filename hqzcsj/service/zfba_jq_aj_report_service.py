from __future__ import annotations

import tempfile
import threading
from datetime import datetime
from pathlib import Path

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_jq_aj_dao


class ZfbaJqAjReportService:
    """
    警情案件统计 - 导出报表（写入 xls 模板）

    说明：
    - 数据来源：hqzcsj/dao/zfba_jq_aj_dao.py（按需求的 SQL 口径）
    - 模板：jingqing_anjian/templates/jqaj_jqajcfcxytj_template.xls
    - 写入方式：Windows Excel COM（确保公式/样式不丢失）
    """

    REPORT_FIXED_TYPES = ["涉黄", "赌博", "打架斗殴", "盗窃"]
    REPORT_REGION_CODES = [
        ("市局", "445300"),
        ("云城", "445302"),
        ("云安", "445303"),
        ("罗定", "445381"),
        ("新兴", "445321"),
        ("郁南", "445322"),
    ]
    REPORT_TYPE_START_ROW = {
        "涉黄": 3,
        "赌博": 13,
        "打架斗殴": 23,
        "盗窃": 33,
    }

    _EXCEL_COM_LOCK = threading.Lock()

    def build_report_xls(self, kssj: str, jssj: str, hbkssj: str, hbjssj: str) -> bytes:
        tbkssj, tbjssj = self.calculate_tb_dates(kssj, jssj)

        repo_root = Path(__file__).resolve().parent.parent.parent
        template_path = repo_root / "jingqing_anjian" / "templates" / "jqaj_jqajcfcxytj_template.xls"
        if not template_path.exists():
            raise FileNotFoundError(f"找不到报表模板: {template_path}")

        with self._EXCEL_COM_LOCK:
            return self._build_report_xls_via_excel_com(
                template_path=template_path,
                kssj=kssj,
                jssj=jssj,
                tbkssj=tbkssj,
                tbjssj=tbjssj,
                hbkssj=hbkssj,
                hbjssj=hbjssj,
            )

    def calculate_tb_dates(self, kssj: str, jssj: str) -> tuple[str, str]:
        """
        同比：上一年同周期（年-1）
        """
        try:
            dt_kssj = datetime.strptime(kssj, "%Y-%m-%d %H:%M:%S")
            dt_jssj = datetime.strptime(jssj, "%Y-%m-%d %H:%M:%S")

            try:
                tbkssj_dt = dt_kssj.replace(year=dt_kssj.year - 1)
            except Exception:
                if dt_kssj.month == 2 and dt_kssj.day == 29:
                    tbkssj_dt = dt_kssj.replace(year=dt_kssj.year - 1, day=28)
                else:
                    raise

            try:
                tbjssj_dt = dt_jssj.replace(year=dt_jssj.year - 1)
            except Exception:
                if dt_jssj.month == 2 and dt_jssj.day == 29:
                    tbjssj_dt = dt_jssj.replace(year=dt_jssj.year - 1, day=28)
                else:
                    raise

            return tbkssj_dt.strftime("%Y-%m-%d %H:%M:%S"), tbjssj_dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError(f"时间格式错误, 期望格式: YYYY-MM-DD HH:MM:SS, 错误: {exc}")

    def _build_report_xls_via_excel_com(
        self,
        *,
        template_path: Path,
        kssj: str,
        jssj: str,
        tbkssj: str,
        tbjssj: str,
        hbkssj: str,
        hbjssj: str,
    ) -> bytes:
        """
        使用 Windows Excel COM 自动化写入模板。
        优点：模板的公式/格式完全保留；只写指定的数字单元格。
        """
        try:
            import win32com.client  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"缺少 pywin32 或当前环境不支持 Excel COM: {exc}")

        try:
            import pythoncom  # type: ignore
        except Exception:
            pythoncom = None

        def _col_letter(n: int) -> str:
            s = ""
            while n > 0:
                n, r = divmod(n - 1, 26)
                s = chr(65 + r) + s
            return s

        def _addr(row: int, col: int) -> str:
            return f"{_col_letter(col)}{row}"

        periods = {
            "cur": (kssj, jssj),
            "tb": (tbkssj, tbjssj),
            "hb": (hbkssj, hbjssj),
        }

        # 模板列定义（沿用 jingqing_anjian 的模板坐标）
        col_jq = {"cur": 2, "tb": 3, "hb": 4}  # B/C/D
        col_xz = {"cur": 7, "tb": 8, "hb": 9}  # G/H/I
        col_xs = {"cur": 12, "tb": 13, "hb": 14}  # L/M/N
        col_zj = {"cur": 17, "tb": 18, "hb": 19}  # Q/R/S
        col_jlz = {"cur": 22, "tb": 23, "hb": 24}  # V/W/X

        excel = None
        wb = None
        tmp_path: Path | None = None
        conn = None
        try:
            if pythoncom is not None:
                pythoncom.CoInitialize()

            conn = get_database_connection()

            # 预取每个类型的 ay_pattern（给案件/文书类统计用）
            patterns_by_type: dict[str, list[str]] = {}
            for leixing in self.REPORT_FIXED_TYPES:
                patterns_by_type[leixing] = zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=[leixing])

            excel = win32com.client.DispatchEx("Excel.Application")
            excel.Visible = False
            excel.DisplayAlerts = False

            wb = excel.Workbooks.Open(str(template_path), 0, True)  # ReadOnly=True
            ws = wb.Worksheets(1)

            for leixing in self.REPORT_FIXED_TYPES:
                start_row = self.REPORT_TYPE_START_ROW[leixing]
                patterns = patterns_by_type.get(leixing) or []

                data_by_period: dict[str, dict[str, object]] = {}
                for pkey, (s, e) in periods.items():
                    jq = zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=s, end_time=e, leixing_list=[leixing])
                    ajxx = zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=s, end_time=e, patterns=patterns)
                    zhiju = zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(conn, start_time=s, end_time=e, patterns=patterns)
                    jlz = zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=s, end_time=e, patterns=patterns)
                    data_by_period[pkey] = {"jq": jq, "ajxx": ajxx, "zhiju": zhiju, "jlz": jlz}

                def _g(m: dict[str, int], code: str) -> int:
                    return int(m.get(code) or 0)

                def _g2(m: dict[str, dict[str, int]], key: str, code: str) -> int:
                    return int((m.get(key) or {}).get(code) or 0)

                # 市局~郁南
                for idx, (_name, region_code) in enumerate(self.REPORT_REGION_CODES):
                    row = start_row + idx
                    for pkey in ("cur", "tb", "hb"):
                        d = data_by_period[pkey]
                        ws.Range(_addr(row, col_jq[pkey])).Value = _g(d["jq"], region_code)  # type: ignore[arg-type]
                        ws.Range(_addr(row, col_xz[pkey])).Value = _g2(d["ajxx"], "行政", region_code)  # type: ignore[arg-type]
                        ws.Range(_addr(row, col_xs[pkey])).Value = _g2(d["ajxx"], "刑事", region_code)  # type: ignore[arg-type]
                        ws.Range(_addr(row, col_zj[pkey])).Value = _g(d["zhiju"], region_code)  # type: ignore[arg-type]
                        ws.Range(_addr(row, col_jlz[pkey])).Value = _g(d["jlz"], region_code)  # type: ignore[arg-type]

                # “所有”行：包含其他地区码；只写 警情/案件，不写文书类
                all_row = start_row + len(self.REPORT_REGION_CODES)
                for pkey in ("cur", "tb", "hb"):
                    d = data_by_period[pkey]
                    jq_all = sum(int(v or 0) for v in (d["jq"] or {}).values())  # type: ignore[union-attr]
                    xz_all = sum(int(v or 0) for v in ((d["ajxx"] or {}).get("行政", {}) or {}).values())  # type: ignore[union-attr]
                    xs_all = sum(int(v or 0) for v in ((d["ajxx"] or {}).get("刑事", {}) or {}).values())  # type: ignore[union-attr]
                    ws.Range(_addr(all_row, col_jq[pkey])).Value = int(jq_all)
                    ws.Range(_addr(all_row, col_xz[pkey])).Value = int(xz_all)
                    ws.Range(_addr(all_row, col_xs[pkey])).Value = int(xs_all)

            try:
                excel.CalculateFull()
            except Exception:
                pass

            tmp_dir = Path(tempfile.gettempdir())
            tmp_path = tmp_dir / f"zfba_jq_aj_report_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.xls"
            wb.SaveAs(str(tmp_path), FileFormat=56)  # 56 = xlExcel8 (.xls)
            wb.Close(SaveChanges=False)
            wb = None
            excel.Quit()
            excel = None

            return tmp_path.read_bytes()
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            try:
                if wb is not None:
                    wb.Close(SaveChanges=False)
            except Exception:
                pass
            try:
                if excel is not None:
                    excel.Quit()
            except Exception:
                pass
            try:
                if pythoncom is not None:
                    pythoncom.CoUninitialize()
            except Exception:
                pass
            try:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

