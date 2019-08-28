
select sum(timestampdiff(second, t1, t0)), datetime0
from (
    select time0 as t0, time1 as t1, datetime0 from tdvt.calcs
) tb1
group by datetime0
order by datetime0;