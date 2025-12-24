# 公共映射服务层
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from gonggong.dao.mapping_dao import MappingDAO


class MappingService:
    def __init__(self):
        self.mapping_dao = MappingDAO()
    
    def map_name_to_district(self, data, name_field='name'):
        """
        将数据中的指定字段与sys_dq_pcs表中的字段进行映射
        
        Args:
            data (list): 数据列表
            name_field (str): 需要映射的字段名，默认为'name'
            
        Returns:
            list: 映射处理后的数据列表
        """
        return self.mapping_dao.map_name_to_district(data, name_field)