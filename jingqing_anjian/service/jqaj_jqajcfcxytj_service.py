# -*- coding: utf-8 -*-
"""
警情案件处罚查询与统计 - 服务层
Service (业务逻辑层)
"""

from datetime import datetime
import io
import csv
from openpyxl import Workbook

from jingqing_anjian.dao.jqaj_jqajcfcxytj_dao import JqajcfcxytjDAO


class JqajcfcxytjService:
    """警情案件处罚查询与统计业务逻辑服务"""

    # 地区代码映射
    REGION_MAP = {
        '445302': '云城',
        '445303': '云安',
        '445381': '罗定',
        '445321': '新兴',
        '445322': '郁南',
        '445300': '市局'
    }

    # 汇总表字段列表
    SUMMARY_COLUMNS = [
        '地区', '警情', '同比警情', '行政', '同比行政',
        '刑事', '同比刑事', '治拘', '同比治拘',
        '刑拘', '同比刑拘', '起诉', '同比起诉',
        '移送人员', '同比移送人员', '移送案件', '同比移送案件'
    ]

    def __init__(self):
        self.dao = JqajcfcxytjDAO()

    def get_case_types(self):
        """
        获取警情类型列表

        Returns:
            list: 警情类型列表
        """
        return self.dao.get_case_types()

    def calculate_tb_dates(self, kssj, jssj):
        """
        计算同比时间段

        Args:
            kssj: 开始时间字符串 (格式: YYYY-MM-DD HH:MM:SS)
            jssj: 结束时间字符串 (格式: YYYY-MM-DD HH:MM:SS)

        Returns:
            tuple: (tbkssj, tbjssj) 同比开始和结束时间
        """
        try:
            dt_kssj = datetime.strptime(kssj, '%Y-%m-%d %H:%M:%S')
            dt_jssj = datetime.strptime(jssj, '%Y-%m-%d %H:%M:%S')

            # 同比年份减1
            tbkssj = dt_kssj.replace(year=dt_kssj.year - 1).strftime('%Y-%m-%d %H:%M:%S')
            tbjssj = dt_jssj.replace(year=dt_jssj.year - 1).strftime('%Y-%m-%d %H:%M:%S')

            return tbkssj, tbjssj
        except ValueError as e:
            raise ValueError(f"时间格式错误, 期望格式: YYYY-MM-DD HH:MM:SS, 错误: {e}")

    def process_summary_stats(self, kssj, jssj, leixing_list):
        """
        处理汇总统计数据

        Args:
            kssj: 开始时间
            jssj: 结束时间
            leixing_list: 警情类型列表

        Returns:
            list: 汇总统计数据列表
        """
        # 计算同比时间
        tbkssj, tbjssj = self.calculate_tb_dates(kssj, jssj)

        # 获取数据 (查询范围从同比开始时间到结束时间, 包含两年数据)
        jingqings = self.dao.get_jingqings(tbkssj, jssj, leixing_list)
        anjians = self.dao.get_anjians(tbkssj, jssj, leixing_list)
        wenshus = self.dao.get_wenshus(tbkssj, jssj, leixing_list)

        # 按地区分组统计
        result = []

        # 获取所有涉及的地区
        regions = set()
        for jq in jingqings:
            diqu = str(jq.get('diqu', ''))
            if diqu:
                regions.add(diqu)
        for aj in anjians:
            diqu = str(aj.get('地区', ''))
            if diqu:
                regions.add(diqu)
        for ws in wenshus:
            region = str(ws.get('region', ''))
            if region:
                regions.add(region)

        # 如果没有数据,返回空列表
        if not regions:
            return []

        # 对每个地区进行统计
        # 收集"其他"地区的数据
        other_data = {
            'jingqing': 0, 'tb_jingqing': 0,
            'xingzheng': 0, 'tb_xingzheng': 0,
            'xingshi': 0, 'tb_xingshi': 0,
            'zhiju': 0, 'tb_zhiju': 0,
            'xingju': 0, 'tb_xingju': 0,
            'qisu': 0, 'tb_qisu': 0,
            'yisong_ry': 0, 'tb_yisong_ry': 0,
            'yisong_aj': 0, 'tb_yisong_aj': 0
        }

        for region_code in regions:
            region_name = self.REGION_MAP.get(region_code)

            # 统计今年数据
            jingqing_count = self._count_jingqings(jingqings, region_code, kssj, jssj)
            xingzheng_count = self._count_anjians(anjians, region_code, '行政', kssj, jssj)
            xingshi_count = self._count_anjians(anjians, region_code, '刑事', kssj, jssj)
            zhiju_count = self._count_zhiju(wenshus, region_code, kssj, jssj)
            xingju_count = self._count_xingju(wenshus, region_code, kssj, jssj)
            qisu_count = self._count_qisu(wenshus, region_code, kssj, jssj)
            yisong_ry_count = self._count_yisong(wenshus, region_code, '01', kssj, jssj)
            yisong_aj_count = self._count_yisong(wenshus, region_code, '04', kssj, jssj)

            # 统计同比数据
            tb_jingqing_count = self._count_jingqings(jingqings, region_code, tbkssj, tbjssj)
            tb_xingzheng_count = self._count_anjians(anjians, region_code, '行政', tbkssj, tbjssj)
            tb_xingshi_count = self._count_anjians(anjians, region_code, '刑事', tbkssj, tbjssj)
            tb_zhiju_count = self._count_zhiju(wenshus, region_code, tbkssj, tbjssj)
            tb_xingju_count = self._count_xingju(wenshus, region_code, tbkssj, tbjssj)
            tb_qisu_count = self._count_qisu(wenshus, region_code, tbkssj, tbjssj)
            tb_yisong_ry_count = self._count_yisong(wenshus, region_code, '01', tbkssj, tbjssj)
            tb_yisong_aj_count = self._count_yisong(wenshus, region_code, '04', tbkssj, tbjssj)

            if region_name:
                # 已映射的地区，直接添加到结果
                result.append({
                    '地区': region_name,
                    '地区代码': region_code,
                    '警情': jingqing_count,
                    '同比警情': tb_jingqing_count,
                    '行政': xingzheng_count,
                    '同比行政': tb_xingzheng_count,
                    '刑事': xingshi_count,
                    '同比刑事': tb_xingshi_count,
                    '治拘': zhiju_count,
                    '同比治拘': tb_zhiju_count,
                    '刑拘': xingju_count,
                    '同比刑拘': tb_xingju_count,
                    '起诉': qisu_count,
                    '同比起诉': tb_qisu_count,
                    '移送人员': yisong_ry_count,
                    '同比移送人员': tb_yisong_ry_count,
                    '移送案件': yisong_aj_count,
                    '同比移送案件': tb_yisong_aj_count
                })
            else:
                # 未映射的地区，累加到"其他"
                other_data['jingqing'] += jingqing_count
                other_data['tb_jingqing'] += tb_jingqing_count
                other_data['xingzheng'] += xingzheng_count
                other_data['tb_xingzheng'] += tb_xingzheng_count
                other_data['xingshi'] += xingshi_count
                other_data['tb_xingshi'] += tb_xingshi_count
                other_data['zhiju'] += zhiju_count
                other_data['tb_zhiju'] += tb_zhiju_count
                other_data['xingju'] += xingju_count
                other_data['tb_xingju'] += tb_xingju_count
                other_data['qisu'] += qisu_count
                other_data['tb_qisu'] += tb_qisu_count
                other_data['yisong_ry'] += yisong_ry_count
                other_data['tb_yisong_ry'] += tb_yisong_ry_count
                other_data['yisong_aj'] += yisong_aj_count
                other_data['tb_yisong_aj'] += tb_yisong_aj_count

        # 如果有"其他"数据，添加到结果
        if other_data['jingqing'] > 0 or other_data['xingzheng'] > 0 or other_data['xingshi'] > 0 or \
           other_data['zhiju'] > 0 or other_data['xingju'] > 0 or other_data['qisu'] > 0 or \
           other_data['yisong_ry'] > 0 or other_data['yisong_aj'] > 0:
            result.append({
                '地区': '其他',
                '地区代码': '999999',
                '警情': other_data['jingqing'],
                '同比警情': other_data['tb_jingqing'],
                '行政': other_data['xingzheng'],
                '同比行政': other_data['tb_xingzheng'],
                '刑事': other_data['xingshi'],
                '同比刑事': other_data['tb_xingshi'],
                '治拘': other_data['zhiju'],
                '同比治拘': other_data['tb_zhiju'],
                '刑拘': other_data['xingju'],
                '同比刑拘': other_data['tb_xingju'],
                '起诉': other_data['qisu'],
                '同比起诉': other_data['tb_qisu'],
                '移送人员': other_data['yisong_ry'],
                '同比移送人员': other_data['tb_yisong_ry'],
                '移送案件': other_data['yisong_aj'],
                '同比移送案件': other_data['tb_yisong_aj']
            })

        return result

    def _count_jingqings(self, jingqings, region_code, kssj, jssj):
        """统计警情数量"""
        count = 0
        seen_caseno = set()
        for jq in jingqings:
            if str(jq.get('diqu', '')) == region_code:
                caseno = jq.get('caseno')
                calltime = jq.get('calltime')
                # 去重计数并检查时间范围
                if caseno and caseno not in seen_caseno:
                    if kssj <= str(calltime) <= jssj:
                        seen_caseno.add(caseno)
                        count += 1
        return count

    def _count_anjians(self, anjians, region_code, ajlx, kssj, jssj):
        """统计案件数量"""
        count = 0
        for aj in anjians:
            if str(aj.get('地区', '')) == region_code:
                if aj.get('案件类型') == ajlx:
                    larq = aj.get('立案日期')
                    # 检查时间范围
                    if larq and kssj <= str(larq) <= jssj:
                        count += 1
        return count

    def _count_zhiju(self, wenshus, region_code, kssj, jssj):
        """统计治拘数量 (zhiju != '0')"""
        count = 0
        for ws in wenshus:
            if str(ws.get('region', '')) == region_code:
                zhiju = str(ws.get('zhiju', '0'))
                spsj = ws.get('spsj')
                # 检查时间和治拘值
                if zhiju != '0' and spsj and kssj <= str(spsj) <= jssj:
                    count += 1
        return count

    def _count_xingju(self, wenshus, region_code, kssj, jssj):
        """统计刑拘数量 (flws_bt包含'拘留证')"""
        count = 0
        seen_wsywxxid = set()
        for ws in wenshus:
            if str(ws.get('region', '')) == region_code:
                wsywxxid = ws.get('wsywxxid')
                flws_bt = ws.get('flws_bt', '')
                spsj = ws.get('spsj')
                # 检查时间、标题和去重
                if wsywxxid and '拘留证' in flws_bt:
                    if wsywxxid not in seen_wsywxxid:
                        if spsj and kssj <= str(spsj) <= jssj:
                            seen_wsywxxid.add(wsywxxid)
                            count += 1
        return count

    def _count_qisu(self, wenshus, region_code, kssj, jssj):
        """统计起诉数量 (flws_bt包含'起诉意见')"""
        count = 0
        seen_wsywxxid = set()
        for ws in wenshus:
            if str(ws.get('region', '')) == region_code:
                wsywxxid = ws.get('wsywxxid')
                flws_bt = ws.get('flws_bt', '')
                spsj = ws.get('spsj')
                if wsywxxid and '起诉意见' in flws_bt:
                    if wsywxxid not in seen_wsywxxid:
                        if spsj and kssj <= str(spsj) <= jssj:
                            seen_wsywxxid.add(wsywxxid)
                            count += 1
        return count

    def _count_yisong(self, wenshus, region_code, dxlxdm, kssj, jssj):
        """统计移送数量 (dxlxdm='01'或'04', flws_bt包含'移送')"""
        count = 0
        seen_wsywxxid = set()
        for ws in wenshus:
            if str(ws.get('region', '')) == region_code:
                wsywxxid = ws.get('wsywxxid')
                ws_dxlxdm = str(ws.get('dxlxdm', ''))
                flws_bt = ws.get('flws_bt', '')
                spsj = ws.get('spsj')
                if wsywxxid and ws_dxlxdm == dxlxdm and '移送' in flws_bt:
                    if wsywxxid not in seen_wsywxxid:
                        if spsj and kssj <= str(spsj) <= jssj:
                            seen_wsywxxid.add(wsywxxid)
                            count += 1
        return count

    def get_detail_data(self, kssj, jssj, leixing_list, click_field, region_code):
        """
        获取明细数据

        Args:
            kssj: 开始时间
            jssj: 结束时间
            leixing_list: 警情类型列表
            click_field: 点击的字段名
            region_code: 地区代码

        Returns:
            tuple: (columns, data) 字段列表和数据列表
        """
        # 根据点击的字段确定数据源
        if click_field in ['警情', '同比警情']:
            # 确定时间范围
            tbkssj, tbjssj = self.calculate_tb_dates(kssj, jssj)
            if '同比' in click_field:
                query_kssj, query_jssj = tbkssj, tbjssj
            else:
                query_kssj, query_jssj = kssj, jssj

            jingqings = self.dao.get_jingqings(query_kssj, query_jssj, leixing_list)
            # 过滤地区
            filtered = [jq for jq in jingqings if str(jq.get('diqu', '')) == region_code]

            columns = ['警情编号', '类型', '报警时间', '地区', '派出所', '警情地址', '处警情况']
            data = []
            for jq in filtered:
                data.append({
                    '警情编号': jq.get('caseno', ''),
                    '类型': jq.get('leixing', ''),
                    '报警时间': str(jq.get('calltime', '')),
                    '地区': jq.get('diqu', ''),
                    '派出所': jq.get('dutyedptname', ''),
                    '警情地址': jq.get('occuraddress', ''),
                    '警情地址': jq.get('casecontents', ''),
                    '处警情况': jq.get('replies', '')
                })
            return columns, data

        elif click_field in ['行政', '同比行政', '刑事', '同比刑事']:
            tbkssj, tbjssj = self.calculate_tb_dates(kssj, jssj)
            if '同比' in click_field:
                query_kssj, query_jssj = tbkssj, tbjssj
            else:
                query_kssj, query_jssj = kssj, jssj

            ajlx = '行政' if '行政' in click_field else '刑事'
            anjians = self.dao.get_anjians(query_kssj, query_jssj, leixing_list)
            # 过滤地区和案件类型
            filtered = [aj for aj in anjians if str(aj.get('地区', '')) == region_code and aj.get('案件类型') == ajlx]

            columns = ['案件编号', '案件名称', '简要案情', '案件类型', '立案日期', '案由', '案件状态']
            data = []
            for aj in filtered:
                data.append({
                    '案件编号': aj.get('案件编号', ''),
                    '地区': aj.get('地区', ''),
                    '案件名称': aj.get('案件名称', ''),
                    '简要案情': aj.get('简要案情', ''),
                    '案件类型': aj.get('案件类型', ''),
                    '立案日期': str(aj.get('立案日期', '')),
                    '案由': aj.get('案由', ''),
                    '案件状态': aj.get('案件状态名称', '')
                })
            return columns, data

        elif click_field in ['治拘', '同比治拘', '刑拘', '同比刑拘', '起诉', '同比起诉', '移送人员', '同比移送人员', '移送案件', '同比移送案件']:
            tbkssj, tbjssj = self.calculate_tb_dates(kssj, jssj)
            if '同比' in click_field:
                query_kssj, query_jssj = tbkssj, tbjssj
            else:
                query_kssj, query_jssj = kssj, jssj

            wenshus = self.dao.get_wenshus(query_kssj, query_jssj, leixing_list)
            # 过滤地区
            filtered = [ws for ws in wenshus if str(ws.get('region', '')) == region_code]

            # 根据点击字段进一步过滤数据
            if click_field in ['治拘', '同比治拘']:
                # 治拘: zhiju 不为 '0'
                filtered = [ws for ws in filtered if str(ws.get('zhiju', '0')) != '0']
            elif click_field in ['刑拘', '同比刑拘']:
                # 刑拘: flws_bt 包含 '拘留证'
                filtered = [ws for ws in filtered if '拘留证' in ws.get('flws_bt', '')]
            elif click_field in ['起诉', '同比起诉']:
                # 起诉: flws_bt 包含 '起诉意见'
                filtered = [ws for ws in filtered if '起诉意见' in ws.get('flws_bt', '')]
            elif click_field in ['移送人员', '同比移送人员']:
                # 移送人员: dxlxdm='01' 且 flws_bt 包含 '移送'
                filtered = [ws for ws in filtered if str(ws.get('dxlxdm', '')) == '01' and '移送' in ws.get('flws_bt', '')]
            elif click_field in ['移送案件', '同比移送案件']:
                # 移送案件: dxlxdm='04' 且 flws_bt 包含 '移送'
                filtered = [ws for ws in filtered if str(ws.get('dxlxdm', '')) == '04' and '移送' in ws.get('flws_bt', '')]

            columns = ['文书编号', '文书标题', '时间', '案件编号', '案件名称', '案由', '警告', '罚款', '治拘']
            data = []
            for ws in filtered:
                data.append({
                    '文书编号': ws.get('flws_dxbh', ''),
                    '地区': ws.get('region', ''),
                    '文书标题': ws.get('flws_bt', ''),
                    '时间': str(ws.get('spsj', '')),
                    '案件编号': ws.get('asjbh', ''),
                    '案件名称': ws.get('asjmc', ''),
                    '案由': ws.get('aymc', ''),
                    '警告': ws.get('jinggao', ''),
                    '罚款': ws.get('fakuan', ''),
                    '治拘': ws.get('zhiju', '')
                })
            return columns, data

        return [], []

    def build_export_data(self, data, columns, format_type):
        """
        构建导出数据

        Args:
            data: 数据列表
            columns: 字段列表
            format_type: 格式类型 ('excel' 或 'csv')

        Returns:
            bytes or str: 导出的数据
        """
        if format_type == 'excel':
            return self._build_excel_content(columns, data)
        else:
            return self._build_csv_content(columns, data)

    def _build_excel_content(self, columns, rows):
        """构建Excel内容"""
        wb = Workbook()
        ws = wb.active
        ws.append(columns)
        for row in rows:
            ws.append([self._normalize_export_value(row.get(col, '')) for col in columns])
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return output.getvalue()

    def _build_csv_content(self, columns, rows):
        """构建CSV内容"""
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([self._normalize_export_value(row.get(col, '')) for col in columns])
        # 添加UTF-8 BOM确保Excel正确识别中文
        return '\ufeff' + buffer.getvalue()

    def _normalize_export_value(self, value):
        """标准化导出值"""
        if value is None:
            return ''
        if isinstance(value, (list, dict)):
            return str(value)
        return value
