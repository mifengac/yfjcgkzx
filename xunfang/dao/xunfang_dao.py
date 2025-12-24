"""
巡防统计数据访问层
处理与巡防统计数据相关的数据库操作
"""
import requests
from typing import Dict, Any, List
from datetime import datetime, timedelta
import logging
from gonggong.service.session_manager import session_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def get_cross_day_data(url: str, begin_time: str, end_time: str) -> List[Dict[str, Any]]:
    """
    获取跨天数据
    """
    params = {
        'keywords': '',
        'deptId': '',
        'deptName': '全部',
        'isBoundary': '',
        'monitorTypeCode': '',
        'deviceStatusCode': '',
        'pageSize': '99999',  # 获取所有数据
        'pageNum': '1',
        'orderByColumn': '',
        'isAsc': 'asc',
        'params[beginTime]': begin_time,
        'params[endTime]': end_time,
        'deploymentType': '',
        'deploymentId': '',
        'deploymentName': '',
        'deploymentTypeCode': '',
        'userTypeCode': '',
        'dutyTypeCode': '',
        'dutyTypeName': '',
        'dutyLevelCode': '',
        'policeCategory': '',
        'userId': '',
        'userName': '',
        'reportState': ''
    }
    
    # 添加日志记录，便于调试
    logging.info(f"请求URL: {url}")
    logging.info(f"请求参数: {params}")
    
    try:
        # 使用全局会话管理器发起请求
        response = session_manager.make_request('POST', url, data=params)
        response.raise_for_status()
        
        # 解析JSON数据
        data = response.json()
        logging.info(f"获取到 {len(data.get('rows', []))} 条数据")
        return data.get('rows', [])
    except Exception as e:
        logging.error(f"获取跨天数据失败: {e}")
        raise