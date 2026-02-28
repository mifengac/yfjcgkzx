import math
import io
import openpyxl
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from .jingqing_api_client import api_client

def haversine_distance(lng1, lat1, lng2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees) in meters
    """
    # Convert decimal degrees to radians 
    try:
        lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
    except (ValueError, TypeError):
        return float('inf')

    # Haversine formula 
    dlng = lng2 - lng1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371000 # Radius of earth in meters
    return c * r

def fetch_all_case_list(base_payload):
    """
    Paginate through the API to fetch all case data.
    """
    all_rows = []
    page_size = 100 # increase page size for efficiency if API allows it. Usually 100 works well.
    current_page = 1
    
    payload = base_payload.copy()
    payload['pageSize'] = page_size
    
    while True:
        payload['pageNum'] = current_page
        result = api_client.get_case_list(payload)
        
        rows = result.get("rows", [])
        total = result.get("total", 0)
        
        if not rows:
            break
            
        all_rows.extend(rows)
        
        if len(all_rows) >= total:
            break
            
        current_page += 1
        
    return all_rows

def fetch_srr_list(payload):
    """
    Fetch srr data directly from the system.
    """
    return api_client.get_srr_list(payload)

def calc_time_period(data):
    """
    Calculate cases by time periods (0-3时,3-6时,...)
    """
    periods = {
        "0-3时": 0, "3-6时": 0, "6-9时": 0, "9-12时": 0,
        "12-15时": 0, "15-18时": 0, "18-21时": 0, "21-24时": 0
    }
    
    for row in data:
        call_time = row.get("callTime")
        if not call_time or len(call_time) < 13:
            continue
            
        try:
            # Example format: "2026-02-28 04:31:29"
            hour = int(call_time[11:13])
            if 0 <= hour < 3:
                periods["0-3时"] += 1
            elif 3 <= hour < 6:
                periods["3-6时"] += 1
            elif 6 <= hour < 9:
                periods["6-9时"] += 1
            elif 9 <= hour < 12:
                periods["9-12时"] += 1
            elif 12 <= hour < 15:
                periods["12-15时"] += 1
            elif 15 <= hour < 18:
                periods["15-18时"] += 1
            elif 18 <= hour < 21:
                periods["18-21时"] += 1
            elif 21 <= hour < 24:
                periods["21-24时"] += 1
        except Exception:
            pass
            
    # Convert to sorted list descending
    sorted_periods = sorted(periods.items(), key=lambda x: x[1], reverse=True)
    return sorted_periods

def calc_duty_dept(data):
    """
    Aggregate by dutyDeptName
    """
    dept_counts = defaultdict(int)
    for row in data:
        dept = row.get("dutyDeptName", "未知")
        if not dept:
            dept = "未知"
        dept_counts[dept] += 1
        
    return sorted(dept_counts.items(), key=lambda x: x[1], reverse=True)

def calc_repeat_phone(data):
    """
    Aggregate by callerPhone
    Clean phone numbers
    """
    phone_counts = defaultdict(int)
    for row in data:
        phone = row.get("callerPhone", "")
        if not phone:
            continue
        # simple cleaning
        cleaned = "".join(c for c in phone if c.isdigit())
        if len(cleaned) < 5:  # skip too short numbers or empty
            continue
        phone_counts[cleaned] += 1
        
    return sorted(
        [(k, v) for k, v in phone_counts.items() if v > 1],
        key=lambda x: x[1], reverse=True
    )

def calc_50m_cluster(data):
    """
    Cluster by Lat/Lng within 50 meters
    Format: occurAddress + ':' + callTime (x次)
    """
    # Sort data by callTime to ensure chronological processing
    sorted_data = sorted(data, key=lambda x: x.get('callTime', ''))
    
    clusters = []
    unprocessed = []
    
    # Filter items that have coordinates
    for row in sorted_data:
        lng = row.get("lngOfCriterion")
        lat = row.get("latOfCriterion")
        if lng and lat:
            unprocessed.append({
                "row": row,
                "lng": float(lng),
                "lat": float(lat)
            })
            
    while unprocessed:
        center = unprocessed.pop(0)
        current_cluster = [center]
        
        # Iterating backward to safely remove
        for i in range(len(unprocessed) - 1, -1, -1):
            target = unprocessed[i]
            dist = haversine_distance(center["lng"], center["lat"], target["lng"], target["lat"])
            if dist <= 50:
                current_cluster.append(target)
                unprocessed.pop(i)
                
        if len(current_cluster) >= 2:
            clusters.append(current_cluster)
            
    result = []
    for c in clusters:
        center_row = c[0]["row"]
        address = center_row.get("occurAddress", "未知地址")
        time_str = center_row.get("callTime", "")
        count = len(c)
        label = f"{address}:{time_str}({count}次)"
        result.append((label, count))
        
    return sorted(result, key=lambda x: x[1], reverse=True)

def generate_excel_report(analysis_results, all_data, dimensions_selected):
    """
    Generate an openpyxl Workbook with 'top summary' and 'bottom raw data' for each sheet
    """
    wb = Workbook()
    # Remove default sheet
    wb.remove(wb.active)
    
    dim_names = {
        "srr": "各地同环比",
        "time": "时段",
        "dept": "派出所",
        "phone": "重复报警电话",
        "cluster": "半径50米内重复报警地址"
    }

    # Define headers for the raw data
    raw_headers = [
        "接警号", "报警时间", "警情级别", "涉案地址", 
        "报警人电话", "管辖单位", "警情状态", "简要案情"
    ]
    
    for dim_key, title in dim_names.items():
        if dim_key not in dimensions_selected:
            continue
            
        ws = wb.create_sheet(title=title[:31]) # Sheet title limit is 31 chars
        
        dim_data = analysis_results.get(dim_key, [])
        
        # 1. Write Summary table at top
        ws.cell(row=1, column=1, value=f"{title}统计表").font = openpyxl.styles.Font(bold=True)
        if dim_key == "srr":
             # SRR is dict structure per list
            ws.cell(row=3, column=1, value="单位名称")
            ws.cell(row=3, column=2, value="本期数")
            ws.cell(row=3, column=3, value="同比上期")
            ws.cell(row=3, column=4, value="同比比例")
            ws.cell(row=3, column=5, value="环比上期")
            ws.cell(row=3, column=6, value="环比比例")
            
            row_idx = 4
            for item in dim_data:
                ws.cell(row=row_idx, column=1, value=item.get("name", ""))
                ws.cell(row=row_idx, column=2, value=item.get("presentCycle", ""))
                ws.cell(row=row_idx, column=3, value=item.get("upperY2yCycle", ""))
                ws.cell(row=row_idx, column=4, value=item.get("y2yProportion", ""))
                ws.cell(row=row_idx, column=5, value=item.get("upperM2mCycle", ""))
                ws.cell(row=row_idx, column=6, value=item.get("m2mProportion", ""))
                row_idx += 1
        else:
            # Others are lists of tuples: [(label, value), ...]
            ws.cell(row=3, column=1, value="统计项")
            ws.cell(row=3, column=2, value="数量")
            
            row_idx = 4
            for item in dim_data:
                ws.cell(row=row_idx, column=1, value=str(item[0]))
                ws.cell(row=row_idx, column=2, value=str(item[1]))
                row_idx += 1
                
        # 2. Write raw data
        start_raw_row = row_idx + 3
        ws.cell(row=start_raw_row, column=1, value="分析源数据明细").font = openpyxl.styles.Font(bold=True)
        
        start_raw_row += 1
        for col_idx, h in enumerate(raw_headers, 1):
            ws.cell(row=start_raw_row, column=col_idx, value=h).font = openpyxl.styles.Font(bold=True)
            
        row_idx = start_raw_row + 1
        for raw_row in all_data:
            ws.cell(row=row_idx, column=1, value=raw_row.get("caseNo", ""))
            ws.cell(row=row_idx, column=2, value=raw_row.get("callTime", ""))
            ws.cell(row=row_idx, column=3, value=raw_row.get("caseLevelName", ""))
            ws.cell(row=row_idx, column=4, value=raw_row.get("occurAddress", ""))
            ws.cell(row=row_idx, column=5, value=raw_row.get("callerPhone", ""))
            ws.cell(row=row_idx, column=6, value=raw_row.get("dutyDeptName", ""))
            ws.cell(row=row_idx, column=7, value=raw_row.get("caseState", ""))
            ws.cell(row=row_idx, column=8, value=raw_row.get("caseContents", ""))
            row_idx += 1
            
        # Adjust some columns width
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['D'].width = 40
        ws.column_dimensions['H'].width = 80

    if not wb.sheetnames:
        wb.create_sheet("无数据")
        
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out
