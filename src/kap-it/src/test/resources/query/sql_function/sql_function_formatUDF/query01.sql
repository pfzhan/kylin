--https://github.com/Kyligence/KAP/issues/13612

select UPD_DATE "UPD_DATE",
       TO_TIMESTAMP("UPD_DATE") "NoFmt",
       To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm:ss') "yyyy-MM-dd HH:mm:ss",
       To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm') "yyyy-MM-dd HH:mm",
       To_Timestamp(UPD_DATE,'yyyy-MM-dd HH') "yyyy-MM-dd HH",
       To_Timestamp(UPD_DATE,'yyyy-MM-dd') "yyyy-MM-dd",
       To_Timestamp(UPD_DATE,'yyyy-MM') "yyyy-MM",
       To_Timestamp(UPD_DATE,'yyyy') "yyyy"
from TEST_CATEGORY_GROUPINGS