--https://github.com/Kyligence/KAP/issues/6631

select week(cal_dt) as doy1
from test_kylin_fact as test_kylin_fact
group by test_kylin_fact.cal_dt
order by doy1