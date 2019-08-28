--conformance=LENIENT

select sum(tmp.ab) from (
  select timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1) as ab
  from tdvt.calcs as calcs
  group by ab
  order by ab
) tmp
