# 服务层
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from jingqing_anjian.dao.case_dao import CaseDAO

class CaseService:
    def __init__(self):
        self.case_dao = CaseDAO()
    
    def get_case_stats_by_type(self, case_type=None, start_time=None, end_time=None):
        """
        调用数据库中的SQL函数get_case_stats_by_type获取案件统计数据
        
        Args:
            case_type (str): 案件类型
            start_time (str): 开始时间 (YYYY-MM-DD HH:MM:SS格式)
            end_time (str): 结束时间 (YYYY-MM-DD HH:MM:SS格式)
            
        Returns:
            list: 查询结果列表
        """
        # 调用DAO层获取数据
        return self.case_dao.get_case_stats_by_type(case_type, start_time, end_time)
    
    def get_case_details_by_params(self, case_type=None, start_time=None, end_time=None):
        """
        调用数据库中的SQL函数get_case_details_by_params获取案件详情数据

        Args:
            case_type (str): 案件类型
            start_time (str): 开始时间 (YYYY-MM-DD HH:MM:SS格式)
            end_time (str): 结束时间 (YYYY-MM-DD HH:MM:SS格式)

        Returns:
            list: 查询结果列表
        """
        # 调用DAO层获取数据
        return self.case_dao.get_case_details_by_params(case_type, start_time, end_time)

    def get_ordered_case_details(self, case_type=None, start_time=None, end_time=None):
        """
        获取按照配置顺序排列的案件详情数据

        Args:
            case_type (str): 案件类型
            start_time (str): 开始时间 (YYYY-MM-DD HH:MM:SS格式)
            end_time (str): 结束时间 (YYYY-MM-DD HH:MM:SS格式)

        Returns:
            dict: 包含字段配置和按配置顺序排列的数据
        """
        # 字段显示配置
        field_config = [
            {"field": "案件编号", "display_name": "案件编号"},
            {"field": "案件名称", "display_name": "案件名称"},
            {"field": "警情编号", "display_name": "警情编号"},
            {"field": "简要案情", "display_name": "简要案情"},
            {"field": "案件发生地址名称", "display_name": "案件发生地址名称"},
            {"field": "案件发生地行政区划名称", "display_name": "案件发生地行政区划名称"},
            {"field": "办案单位名称", "display_name": "办案单位名称"},
            {"field": "立案日期", "display_name": "立案日期"},
            {"field": "案由", "display_name": "案由"},
            {"field": "案件类型", "display_name": "案件类型"},
            {"field": "案件状态名称", "display_name": "案件状态名称"},
            {"field": "地区", "display_name": "地区"},
            {"field": "调解人数", "display_name": "调解人数"},
            {"field": "行政罚款人数", "display_name": "行政罚款人数"},
            {"field": "行政拘留人数", "display_name": "行政拘留人数"},
            {"field": "刑拘人数", "display_name": "刑拘人数"},
            {"field": "未成年人", "display_name": "未成年人"}
        ]

        # 获取原始数据
        raw_data = self.case_dao.get_case_details_by_params(case_type, start_time, end_time)

        # 按照配置顺序重新组织数据
        ordered_data = []
        field_names = [item["field"] for item in field_config]

        for row in raw_data:
            ordered_row = {}
            for field_name in field_names:
                ordered_row[field_name] = row.get(field_name, "")
            ordered_data.append(ordered_row)

        return {
            "field_config": field_config,
            "data": ordered_data
        }

    def get_case_ry_data(self, case_type=None, start_time=None, end_time=None):
        """
        获取人员详情数据，列名按数据库函数返回顺序排列

        Args:
            case_type (str): 案件类型
            start_time (str): 开始时间
            end_time (str): 结束时间

        Returns:
            dict: {columns: [..], data: [...]}
        """
        columns, data = self.case_dao.get_case_ry_data(case_type, start_time, end_time)
        return {"columns": columns, "data": data}
