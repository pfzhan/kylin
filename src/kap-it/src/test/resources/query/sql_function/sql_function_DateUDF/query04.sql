select UNIX_TIMESTAMP(CAL_DT),
       UNIX_TIMESTAMP(CAL_DT,'yyyy-MM-dd')
from edw.test_cal_dt