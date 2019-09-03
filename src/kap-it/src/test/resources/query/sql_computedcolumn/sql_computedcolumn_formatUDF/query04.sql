

SELECT count(distinct to_date(UPD_DATE,'yyyy-MM-dd')),
	   count(distinct to_date(UPD_DATE,'yyyy-MM')),
	   count(distinct to_date(UPD_DATE,'yyyy'))
from TEST_CATEGORY_GROUPINGS