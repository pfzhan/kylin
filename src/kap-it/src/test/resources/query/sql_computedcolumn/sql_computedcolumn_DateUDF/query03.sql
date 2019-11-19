select count(distinct UNIX_TIMESTAMP(CAL_DT)),
       count(distinct UNIX_TIMESTAMP(CAL_DT,'yyyy-MM-dd')),
       count(UNIX_TIMESTAMP(CAL_DT)),
       count(UNIX_TIMESTAMP(CAL_DT,'yyyy-MM-dd'))
from edw.test_cal_dt