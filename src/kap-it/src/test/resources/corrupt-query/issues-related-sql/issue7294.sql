--https://github.com/kyligence/kap/issues/7294

select sum({fn convert(item_count, sql_bigint)}) bigint0
from test_kylin_fact
where cal_dt >= date'2014-01-01'
group by cal_dt