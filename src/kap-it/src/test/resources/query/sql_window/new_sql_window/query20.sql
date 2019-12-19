select
lag(cal_dt,50,date'2019-01-01') over (partition by seller_id order by item_count)
,lag(cal_dt,50,'2019-01-01') over (partition by seller_id order by item_count)
,lead(item_count,1,'2019') over (partition by seller_id order by cal_dt)
,lead(item_count,1,2019) over (partition by seller_id order by cal_dt)
,lead(price,1,1.234) over (partition by seller_id order by cal_dt)
from test_kylin_fact