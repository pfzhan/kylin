-- test functions: year, month, quarter, day, hour, minute, second

select count(distinct year(date0)), max(extract(year from date1)),
       count(distinct month(date0)), max(extract(month from date1)),
       count(distinct quarter(date0)), max(extract(quarter from date1)),
       count(distinct hour(date0)), max(extract(hour from date1)),
       count(distinct minute(date0)), max(extract(minute from date1)),
       count(distinct second(date0)), max(extract(second from date1)),
       count(dayofmonth(date0)), max(extract(day from date1))
from tdvt.calcs as calcs