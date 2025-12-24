from openpyxl import Workbook

wb = Workbook()
ws = wb.active
ws.title = '用户权限导入'
ws.append(['username','password','module'])
wb.save('user_permission_import_template.xlsx')
print('Excel导入模板已生成: user_permission_import_template.xlsx')
