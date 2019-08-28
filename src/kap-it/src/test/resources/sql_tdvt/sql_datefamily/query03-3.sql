
select count(cast(timestampdiff(second, time0, time1) as varchar )) as c1
from tdvt.calcs