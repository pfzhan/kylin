

SELECT count(to_date(UPD_DATE,'yyyy-MM-dd')),
	   count(to_date(UPD_DATE,'yyyy-MM')),
	   count(to_date(UPD_DATE,'yyyy'))
from TEST_CATEGORY_GROUPINGS