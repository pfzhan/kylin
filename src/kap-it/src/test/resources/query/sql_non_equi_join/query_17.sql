-- right join
Select max(test_kylin_fact.price),test_kylin_fact.cal_dt,test_kylin_fact.order_id
From test_kylin_fact
Right join edw.test_cal_dt
On test_kylin_fact.order_id>edw.test_cal_dt.AGE_FOR_YEAR_ID
Group by test_kylin_fact.cal_dt,test_kylin_fact.order_id
order by test_kylin_fact.cal_dt desc ,test_kylin_fact.order_id desc ;
