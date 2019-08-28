-- group by case when contains timestampdiff

select case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end
from tdvt.calcs
group by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end
order by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end