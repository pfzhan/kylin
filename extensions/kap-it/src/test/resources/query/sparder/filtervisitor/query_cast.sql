select  cal_dt,count(*) as TRANS_CNT 
 from test_kylin_fact 
 group by test_kylin_fact.cal_dt
 having cast(sum(price) as BIGINT) > 100

