-- basic test for timestampdiff & timestampadd

select sum(timestampdiff(second, time0, time1) ) as c1,
count(timestampdiff(minute , time0, time1)) as c2,
max(timestampdiff(hour, time1, time0)) as c3,
count(timestampdiff(day, time0, time1)) as c4,
count(timestampdiff(week, time0, time1)) as c5,
count(timestampdiff(month, time0, time1)) as c6,
count(timestampdiff(quarter, time0, time1)) as c7,
count(timestampdiff(year, time0, time1)) as c8,

min(timestampadd(second, 1, time1)) as c9,
count(distinct timestampadd(minute, 1, time1)) as c10,
count(timestampadd(hour, 21, time0)) as c11,
count(timestampadd(day, 31, time0)) as c12,
count(timestampadd(week, 11, time0)) as c13,
count(timestampadd(month, 9, time0)) as c14,
count(timestampadd(quarter, 19, time1)) as c15,
count(timestampadd(year, 29, time1)) as c16
from tdvt.calcs;