
select sum(case when time1 between '' and '2012-11-23' then 1
           end),
       sum(case when time2 between '' and '2012-11-23' then 1
           end)
from TEST_MEASURE