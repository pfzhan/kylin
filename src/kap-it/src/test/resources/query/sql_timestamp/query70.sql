

select sum(case when time2 = '2012-11-23' then 1
                when time2 >= '' then 2
                when time2 > '2012-11-23' then 3
                when time2 < 'A' then 4
                when time2 <= '2012-11-23' then 5
           end)
from TEST_MEASURE