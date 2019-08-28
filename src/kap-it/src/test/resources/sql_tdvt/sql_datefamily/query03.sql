-- basic test for timestampdiff & timestampadd

select sum(timestampdiff(second, time0, time1) ) as c1,
count(distinct timestampadd(minute, 1, time1)) as c2,
max(timestampdiff(hour, time1, time0)) as c3,
min(timestampadd(second, 1, time1)) as c4,
avg(timestampdiff(hour, time0, time1)) as c5
from tdvt.calcs;