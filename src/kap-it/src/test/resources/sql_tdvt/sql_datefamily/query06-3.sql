-- group by case when contains timestampdiff

select case when int0 > 100 then timestampdiff(second, time0, time1)
                when int0 > 50 then timestampdiff(minute, time0, time1)
                when int0 > 0 then timestampdiff(hour, time0, time1) else null end
from tdvt.calcs group by case when int0 > 100 then timestampdiff(second, time0, time1)
                when int0 > 50 then timestampdiff(minute, time0, time1)
                when int0 > 0 then timestampdiff(hour, time0, time1) else null end