--https://github.com/Kyligence/KAP/issues/13612

select to_char(CAL_DT,'YEAR'),
	   to_char(CAL_DT,'MONTH'),
	   to_char(CAL_DT,'DAY')
from TEST_KYLIN_FACT