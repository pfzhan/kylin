select TRANS_ID, count(weekofyear(CAL_DT))
from test_kylin_fact
group by TRANS_ID
order by TRANS_ID
limit 10