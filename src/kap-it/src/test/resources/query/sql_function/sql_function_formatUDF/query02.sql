--https://github.com/Kyligence/KAP/issues/13612

select to_char(CAL_DT,'YEAR'),
       to_char(CAL_DT,'y'),
	   to_char(CAL_DT,'Y'),
	   to_char(CAL_DT,'MONTH'),
	   to_char(CAL_DT,'M'),
	   to_char(CAL_DT,'DAY'),
	   to_char(CAL_DT,'D'),
       to_char(CAL_DT,'d'),
	   to_char(CAL_DT,'h'),
	   to_char(CAL_DT,'m'),
	   to_char(CAL_DT,'s')
from TEST_KYLIN_FACT