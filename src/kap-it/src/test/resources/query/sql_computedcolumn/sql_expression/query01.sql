
select CAL_DT,
       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 end),
       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else 3 end),
       sum(case when ORDER_ID in (1, 2) then 2 else 3 end),
       sum(case when PRICE in (1.1, 2.2) then 2 else 3 end)
from TEST_KYLIN_FACT
group by CAL_DT