
select case when int0 > 10 then sum(timestampdiff(second, time0, time1)) else sum(timestampdiff(minute, time0, time1)) end
from tdvt.calcs
group by int0;