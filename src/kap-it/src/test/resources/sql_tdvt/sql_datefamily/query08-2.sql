select * from (
  select sum(timestampdiff(second, time1, time0)), datetime0
  from (
    select time0, time1, datetime0 from tdvt.calcs
  ) tb1
  group by datetime0
);