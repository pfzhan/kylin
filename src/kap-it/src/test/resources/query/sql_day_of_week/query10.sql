select week('2012-01-01')
,week(cal_dt)
,week(cast(cal_dt as varchar))
,month('2012-01-01')
,month(cal_dt)
,month(cast(cal_dt as varchar))
,quarter('2012-01-01')
,quarter(cal_dt)
,quarter(cast(cal_dt as varchar))
,year('2012-01-01')
,year(cal_dt)
,year(cast(cal_dt as varchar))
,dayofyear('2012-01-01')
,dayofyear(cal_dt)
,dayofyear(cast(cal_dt as varchar))
,dayofmonth('2012-01-01')
,dayofmonth(cal_dt)
,dayofmonth(cast(cal_dt as varchar))
,dayofweek('2012-01-01')
,dayofweek(cal_dt)
,dayofweek(cast(cal_dt as varchar))
,dayofweek('2012-01-01 00:10:00')
,dayofweek(cast(cal_dt as timestamp))
from test_kylin_fact

