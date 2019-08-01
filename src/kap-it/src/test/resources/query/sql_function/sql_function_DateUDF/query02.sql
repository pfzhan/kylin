--https://github.com/Kyligence/KAP/issues/13614

select CAL_DT,date_part('YEAR',CAL_DT)
from TEST_KYLIN_FACT
order by date_part('YEAR',CAL_DT)