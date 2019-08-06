--https://github.com/Kyligence/KAP/issues/13612

SELECT to_date(UPD_DATE,'yyyy-MM-dd'),
	   to_date(UPD_DATE,'yyyy-MM'),
	   to_date(UPD_DATE,'yyyy')
from TEST_CATEGORY_GROUPINGS