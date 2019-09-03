--https://github.com/Kyligence/KAP/issues/13614

select
       --add_months(CAL_DT,2),
	   date_part('YEAR',CAL_DT),
	   date_part('MONTH',CAL_DT),
	   date_part('DAY',CAL_DT),
	   DATE_TRUNC('YEAR', "CAL_DT"),
	   DATE_TRUNC('MONTH', "CAL_DT"),
	   DATE_TRUNC('DAY', "CAL_DT"),
	   datediff(CAL_DT,date'2019-8-3'),
	   hour(CAL_DT)
from TEST_KYLIN_FACT