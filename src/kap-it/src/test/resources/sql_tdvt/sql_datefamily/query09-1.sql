select sum(tmp.ab) from (
  select case when int0 > 0 then timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)
   else 0 end as ab
  from tdvt.calcs as calcs
  group by ab
  order by ab
) tmp