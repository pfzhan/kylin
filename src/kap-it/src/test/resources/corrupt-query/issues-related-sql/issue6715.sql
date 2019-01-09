--https://github.com/Kyligence/KAP/issues/6715

select trans_id, sum(price)
from test_kylin_fact where cal_dt='2012-01-01'
group by trans_id