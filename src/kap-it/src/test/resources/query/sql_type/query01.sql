select cast(sum(price * item_count) as decimal) as d1
from test_kylin_fact
group by cal_dt
order by d1 desc
