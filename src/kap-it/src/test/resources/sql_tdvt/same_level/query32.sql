select DATETIME1, DATETIME0
from TDVT.CALCS
where 1=1
and cast(DATETIME0 as date) >= date'2004-07-28'
and cast(DATETIME1 as timestamp) >= timestamp'2004-07-28 12:34:00'
and date'2004-07-28'<= cast(DATETIME0 as date)
and timestamp'2004-07-28 12:34:00' <= cast(DATETIME1 as timestamp)
group by DATETIME1,DATETIME0