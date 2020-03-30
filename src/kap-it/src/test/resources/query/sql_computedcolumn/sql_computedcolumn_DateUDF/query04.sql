select
	    count(DATE_TRUNC('YEAR', "CAL_DT")),
	    count(DATE_TRUNC('YYYY', "CAL_DT")),
	    count(DATE_TRUNC('YY', "CAL_DT")),
	    count(DATE_TRUNC('MM', "CAL_DT")),
	    count(DATE_TRUNC('MONTH', "CAL_DT")),
	    count(DATE_TRUNC('DAY', "CAL_DT")),
	    count(DATE_TRUNC('DD', "CAL_DT")),
	    count(DATE_TRUNC('HOUR', "CAL_DT")),
	    count(DATE_TRUNC('WEEK', "CAL_DT")),

	    count(distinct DATE_TRUNC('YEAR', "CAL_DT")),
	    count(distinct DATE_TRUNC('YYYY', "CAL_DT")),
	    count(distinct DATE_TRUNC('YY', "CAL_DT")),
	    count(distinct DATE_TRUNC('MM', "CAL_DT")),
	    count(distinct DATE_TRUNC('MONTH', "CAL_DT")),
	    count(distinct DATE_TRUNC('DAY', "CAL_DT")),
	    count(distinct DATE_TRUNC('DD', "CAL_DT")),
	    count(distinct DATE_TRUNC('HOUR', "CAL_DT")),
	    count(distinct DATE_TRUNC('WEEK', "CAL_DT"))
from TEST_KYLIN_FACT