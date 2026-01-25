# 公共映射数据访问层
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gonggong.config.database import DB_CONFIG, get_database_connection


class MappingDAO:
    def __init__(self):
        self.db_config = DB_CONFIG

    def get_connection(self):
        """获取数据库连接"""
        try:
            connection = get_database_connection()
            
            # 设置搜索路径到指定的schema
            cursor = connection.cursor()
            cursor.execute(f"SET search_path TO {self.db_config['schema']}")
            cursor.close()
            
            return connection
        except Exception as e:
            print(f"数据库连接失败: {e}")
            raise

    def map_name_to_district(self, data, name_field='name'):
        """
        将数据中的指定字段与sys_dq_pcs表中的字段进行映射
        
        Args:
            data (list): 数据列表
            name_field (str): 需要映射的字段名，默认为'name'
            
        Returns:
            list: 映射处理后的数据列表
        """
        if not data:
            return data

        # 检查是否有指定字段
        if name_field not in [key for key in data[0]]:
            return data

        # 获取所有唯一的name值
        unique_names = set()
        for record in data:
            if name_field in record and record[name_field]:
                unique_names.add(str(record[name_field]))
        
        if not unique_names:
            return data

        try:
            # 建立数据库连接
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 查询sys_dq_pcs表获取映射
            placeholders = ','.join(['%s'] * len(unique_names))
            query = f"SELECT code, name FROM sys_dq_pcs WHERE code IN ({placeholders})"
            cursor.execute(query, list(unique_names))
            mapping_results = cursor.fetchall()
            
            # 创建映射字典
            name_mapping = {}
            for code, name in mapping_results:
                if code:
                    name_mapping[str(code)] = name
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            # 应用映射到原始数据
            for record in data:
                if name_field in record and str(record[name_field]) in name_mapping:
                    record[name_field] = name_mapping[str(record[name_field])]
            
            return data
        except Exception as e:
            print(f"映射name字段时发生错误: {e}")
            # 发生错误时返回原始数据，不中断程序
            return data
