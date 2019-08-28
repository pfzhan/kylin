-- case when contains timestampdiff

select sum(case when int0 > 0 then timestampdiff(day, time0, time1) end) as ab
  from tdvt.calcs as calcs