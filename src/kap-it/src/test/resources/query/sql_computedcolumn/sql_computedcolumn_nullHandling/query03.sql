

select CAL_DT,sum(ifnull(PRICE,3.0))
from TEST_KYLIN_FACT
group by CAL_DT
order by sum(ifnull(PRICE,3.0)) limit 10