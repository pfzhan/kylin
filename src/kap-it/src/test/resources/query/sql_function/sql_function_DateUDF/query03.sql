select UNIX_TIMESTAMP(time1),
       UNIX_TIMESTAMP(time1,'yyyy-MM-dd'),
       UNIX_TIMESTAMP(time2),
       UNIX_TIMESTAMP(time2,'yyyy-MM-dd')
from test_measure