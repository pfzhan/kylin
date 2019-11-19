select count(UNIX_TIMESTAMP(time1)),
       count(UNIX_TIMESTAMP(time1,'yyyy-MM-dd')),
       count(UNIX_TIMESTAMP(time2)),
       count(UNIX_TIMESTAMP(time2,'yyyy-MM-dd')),
       count(distinct UNIX_TIMESTAMP()),
       count(distinct UNIX_TIMESTAMP(time1)),
       count(distinct UNIX_TIMESTAMP(time1,'yyyy-MM-dd')),
       count(distinct UNIX_TIMESTAMP(time2)),
       count(distinct UNIX_TIMESTAMP(time2,'yyyy-MM-dd'))
from test_measure