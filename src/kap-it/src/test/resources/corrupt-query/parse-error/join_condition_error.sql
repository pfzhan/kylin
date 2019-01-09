

select price, test_kylin_fact.cal_dt from test_kylin_fact
join edw.test_cal_dt as test_cal_dt on test_kylin_fact.cal_dt  = test_cal_dt.year_of_cal_id
where test_kylin_fact.cal_dt < '20120201'
