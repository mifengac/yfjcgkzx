# 任务:帮我将weichengnianren-djdo\wcnr_djdo\dao.py中的ywdata."mv_zfba_all_ajxx"数据源全部换为"ywdata"."zq_zfba_wcnr_ajxx",
  ```SELECT zzwx."ajxx_join_ajxx_ajbh" 案件编号, zzwx."xyrxx_sfzh" 身份证号,zzwx."ajxx_join_ajxx_ajlx" 案件类型,zzwx."ajxx_join_ajxx_ajmc" 案件名称,zzwx."xyrxx_ay_mc" 案由名称,LEFT(zzwx."ajxx_join_ajxx_cbdw_bh",6) diqu,zzwx."ajxx_join_ajxx_lasj" 立案时间 ,zzwx."xyrxx_nl" 年龄 ,zzwx."xyrxx_hjdxz" 户籍地,zzwx."xyrxx_jzdz" 居住地 FROM "zq_zfba_wcnr_xyr" zzwx WHERE zzwx."ajxx_join_ajxx_lasj"```

  1. 在"警情转案率","场所发案率"这两个数据源将ywdata."mv_zfba_all_ajxx"数据源全部换为"ywdata"."zq_zfba_wcnr_ajxx"
  2. 在采取矫治教育措施率,涉刑人员送学率,责令加强监护率,纳管人员再犯率,嫌疑人信息可以直接在"ywdata"."zq_zfba_wcnr_xyr"表获取,不需要通过"mv_minor_person" 与 "mv_zfba_all_ajxx"关联,