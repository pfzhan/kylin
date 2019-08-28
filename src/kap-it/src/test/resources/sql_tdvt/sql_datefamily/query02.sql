-- test functions: dayofyear, dayofmonth, dayofweek, week

select count(week(date0)), max(extract(week from date1)),
       count(dayofyear(date0)), --max(extract(doy from date1)),
       count(dayofmonth(date0)), max(extract(day from date1)),
       count(dayofweek(date0)) --, max(extract(dow from date1))
from tdvt.calcs as calcs