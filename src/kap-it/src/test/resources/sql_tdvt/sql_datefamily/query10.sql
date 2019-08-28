--union

select sum(timestampdiff(minute, time1, time0)), datetime0, time1, time0
from tdvt.calcs group by datetime0, time1, time0
union
select max(timestampdiff(minute, time1, time0)), datetime0, time1, time0
from tdvt.calcs group by datetime0, time1, time0;