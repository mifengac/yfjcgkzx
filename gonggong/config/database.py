# 数据库配置文件

import os
import logging
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# 数据库连接配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST','127.0.0.1'),
    'port': int(os.getenv('DB_PORT')),  # 人大金仓默认端口
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'schema': os.getenv('DB_SCHEMA', 'ywdata')
}

# 数据库连接字符串
DB_CONNECTION_STRING = f"kingbase://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def get_database_connection():
    """
    获取数据库连接的公共函数，包含连接超时设置
    """
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG.get('port', 54321),
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            connect_timeout=30,  # 连接超时30秒
            options="-c statement_timeout=300000"  # 查询超时5分钟（300秒）
        )
        logging.info("数据库连接成功")
        return connection
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        raise e


def execute_query(query: str, params: tuple = None):
    """
    执行数据库查询的公共函数
    """
    connection = None
    cursor = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        results = cursor.fetchall()

        # 获取列名
        columns = [desc[0] for desc in cursor.description]

        # 将结果转换为字典列表
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))

        return data

    except Exception as e:
        logging.error(f"执行查询失败: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
