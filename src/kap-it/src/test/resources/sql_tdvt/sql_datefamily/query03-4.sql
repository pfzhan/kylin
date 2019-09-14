-- replace failed for translated cc expression is timestampdiff(day, cast(date0 as timestamp(0)), date '2012-01-01')
-- https://github.com/Kyligence/KAP/issues/14474

select
timestampdiff(day, date0, date '2012-01-01'),
timestampdiff(minute, date0, timestamp '1998-01-01 00:00:23'),
timestampdiff(week, cast(date0 as timestamp), cast(time1 as timestamp)),
timestampdiff(year, cast(time0  as timestamp), cast(datetime0 as timestamp))
from tdvt.calcs;