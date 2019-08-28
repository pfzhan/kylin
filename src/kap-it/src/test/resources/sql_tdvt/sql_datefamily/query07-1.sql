
with ca as(select *from tdvt.calcs)

select sum(timestampdiff(minute, ca.time1, ca.time0)), ca.datetime0
from ca
group by ca.datetime0