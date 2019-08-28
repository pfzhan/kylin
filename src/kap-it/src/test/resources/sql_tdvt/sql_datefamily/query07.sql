with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs)

select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0
from ca
group by ca.datetime0