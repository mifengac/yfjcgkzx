# 数据库访问层
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from gonggong.config.database import DB_CONFIG
import psycopg2
from datetime import datetime

class CaseDAO:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        """获取数据库连接"""
        try:
            connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config.get('port', 54321),  # 人大金仓默认端口
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            # 设置搜索路径到指定的schema
            cursor = connection.cursor()
            cursor.execute(f"SET search_path TO {self.db_config['schema']}")
            cursor.close()
            
            return connection
        except Exception as e:
            print(f"数据库连接失败: {e}")
            raise
    
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
        try:
            # 建立数据库连接
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 构建调用SQL函数的查询语句
            # 注意：这里假设函数返回一个结果集，需要使用SELECT * FROM来调用
            query = "SELECT * FROM get_case_stats_by_type(%s, %s, %s)"
            
            # 执行查询
            cursor.execute(query, (case_type, start_time, end_time))
            results = cursor.fetchall()
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
            # 将结果转换为字典列表
            case_stats = []
            for row in results:
                case_stat = dict(zip(columns, row))
                # 将所有datetime对象转换为字符串
                for key, value in case_stat.items():
                    if isinstance(value, datetime):
                        case_stat[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                case_stats.append(case_stat)
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            return case_stats
            
        except Exception as e:
            print(f"调用get_case_stats_by_type函数时发生错误: {e}")
            # 返回空列表而不是抛出异常，确保应用不会崩溃
            return []

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
        try:
            # 建立数据库连接
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 构建调用SQL函数的查询语句
            # 注意：这里假设函数返回一个结果集，需要使用SELECT * FROM来调用
            query = "SELECT * FROM get_case_details_by_params(%s, %s, %s)"
            
            # 执行查询
            cursor.execute(query, (case_type, start_time, end_time))
            results = cursor.fetchall()
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
            # 将结果转换为字典列表
            case_details = []
            for row in results:
                case_detail = dict(zip(columns, row))
                # 将所有datetime对象转换为字符串
                for key, value in case_detail.items():
                    if isinstance(value, datetime):
                        case_detail[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                case_details.append(case_detail)
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            return case_details
            
        except Exception as e:
            print(f"调用get_case_details_by_params函数时发生错误: {e}")
            # 返回空列表而不是抛出异常，确保应用不会崩溃
            return []

    def get_case_ry_data(self, case_type=None, start_time=None, end_time=None):
        """
        调用数据库中的SQL函数get_case_ry_data获取人员详情数据

        Args:
            case_type (str): 案件类型
            start_time (str): 开始时间 (YYYY-MM-DD HH:MM:SS格式)
            end_time (str): 结束时间 (YYYY-MM-DD HH:MM:SS格式)

        Returns:
            tuple[list[str], list[dict]]: (列名顺序列表, 数据列表)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 假设数据库函数返回结果集
            query = "SELECT * FROM get_case_ry_data(%s, %s, %s)"
            cursor.execute(query, (case_type, start_time, end_time))
            results = cursor.fetchall()

            # 列名顺序
            columns = [desc[0] for desc in cursor.description]

            data_list = []
            for row in results:
                item = dict(zip(columns, row))
                # 统一将datetime转换为字符串
                for k, v in item.items():
                    if isinstance(v, datetime):
                        item[k] = v.strftime('%Y-%m-%d %H:%M:%S')
                data_list.append(item)

            cursor.close()
            conn.close()

            return columns, data_list
        except Exception as e:
            print(f"调用get_case_ry_data函数时发生错误: {e}")
            return [], []
