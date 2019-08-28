

select count(timestampadd(second, 1+2, time0)),
  count(timestampadd(minute, int0+1, time1))
from tdvt.calcs;