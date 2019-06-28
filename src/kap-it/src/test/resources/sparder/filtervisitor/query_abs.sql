select  cal_dt,count(*) as TRANS_CNT 
 from test_kylin_fact 
 where abs(seller_id) > 10
 group by test_kylin_fact.cal_dt
 --having avg(price) > 100

