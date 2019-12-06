
select count(CAL_DT)
from TEST_KYLIN_FACT
group by case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 end