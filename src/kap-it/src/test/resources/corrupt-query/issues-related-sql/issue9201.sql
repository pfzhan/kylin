--https://github.com/Kyligence/KAP/issues/9201

-- SELECT APPNAME, APTYPE, SUM(BATTERYGASGAUGE) AS BATTERYGASGAUGE
-- 	, SUM(USETIME) AS USETIME
-- FROM BIAPS.POWER_ANALYSE_DEVANDAPP_KPIVALUE_BAT_VER A
-- WHERE (BATCHID = '20181123032600878'
-- 	AND DEVICENAME = 'MODEM'
-- 	AND 'CYCLE' = 1)
-- GROUP BY APPNAME, APTYPE


select lstg_format_name, trans_id, sum(price) as sum_price, sum(item_count) as sum_count
from test_kylin_fact
where (cal_dt='20120201' and seller_id = 20 and lstg_site_id=10)
group by lstg_format_name, trans_id