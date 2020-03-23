-- KE-7752 #14812 select * on subquery with duplicated named cols
SELECT * FROM (
SELECT sum(test_kylin_fact.price), test_kylin_fact.order_id as order_id, test_kylin_fact.cal_dt, edw.test_cal_dt.cal_dt
FROM test_kylin_fact LEFT JOIN edw.test_cal_dt ON test_kylin_fact.order_id = test_cal_dt.DAY_OF_CAL_ID
group by order_id, test_kylin_fact.cal_dt, edw.test_cal_dt.cal_dt
) T