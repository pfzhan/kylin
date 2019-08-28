--

select sum((int1-int2)/(int1+int2)) as c1,
sum((int1-int2)/timestampdiff(second, time0, time1) ) as c2,
sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as c3
from tdvt.calcs;
