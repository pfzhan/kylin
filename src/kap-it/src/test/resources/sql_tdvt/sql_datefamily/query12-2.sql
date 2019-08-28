-- not the param of window function timestampadd related expression will propose computed column

select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, time1)) MAXTIME,
    max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1
from tdvt.calcs
where num1 > 0
group by num1, time0
order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')