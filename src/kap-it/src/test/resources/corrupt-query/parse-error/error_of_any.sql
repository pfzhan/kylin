

select price
from test_kylin_fact
where lstg_format_name = any (select cal_dt, lstg_format_name from test_kylin_fact where seller_id < 10000000);