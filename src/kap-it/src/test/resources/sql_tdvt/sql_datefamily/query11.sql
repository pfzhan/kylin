-- arithmetic operation of aggregations with timestampdiff as param

select max(timestampdiff(second, time0, cast(datetime1 as timestamp)))
            - min(timestampdiff(second, time0, cast(datetime1 as timestamp)))
from tdvt.calcs;