
select  --add_months(CAL_DT,2),
	   count(date_part('YEAR',CAL_DT)),
	   count(date_part('MONTH',CAL_DT)),
	   count(date_part('DAY',CAL_DT)),
	   count(DATE_TRUNC('YEAR', "CAL_DT")),
	   count(DATE_TRUNC('MONTH', "CAL_DT")),
	   count(DATE_TRUNC('DAY', "CAL_DT")),
	   count(datediff(CAL_DT,date'2019-8-3'))
from TEST_KYLIN_FACT