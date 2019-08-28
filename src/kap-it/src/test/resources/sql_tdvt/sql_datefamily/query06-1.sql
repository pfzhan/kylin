-- case when contains timestampdiff

select sum(case when time0 <> time1 then (int2-int1)/timestampdiff(second, time0, time1) * 60
                else (int2 - int1)/ timestampdiff(second, time1, datetime0)*60 end)
from tdvt.calcs;