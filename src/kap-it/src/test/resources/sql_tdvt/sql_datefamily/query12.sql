-- timestampadd on constant appears in window function

select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) MAXTIME,
      max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) over() MAXTIME1
from tdvt.calcs where num1 > 0
group by num1
order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP '1970-01-01 10:01:01');

