select
       ceil(time2 TO year),floor(time2 TO year),
       ceil(time2 to month),floor(time2 to month),
       ceil(time2 to day),floor(time2 to day),
       ceil(time2 to week),floor(time2 to week),
       ceil(time2 to HOUR),floor(time2 to HOUR),
       ceil(time2 to MINUTE),floor(time2 to MINUTE),
       ceil(time2 to SECOND),floor(time2 to SECOND),
       floor(floor(time2 to HOUR) to HOUR),
       ceil(ceil(time2 to HOUR) to HOUR),
       floor(ceil(time2 to HOUR) to HOUR),
       ceil(floor(time2 to HOUR) to HOUR)
from test_measure