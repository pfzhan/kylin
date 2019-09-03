

select count(distinct TO_TIMESTAMP("UPD_DATE")) "NoFmt",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm:ss')) "yyyy-MM-dd HH:mm:ss",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm')) "yyyy-MM-dd HH:mm",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH')) "yyyy-MM-dd HH",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd')) "yyyy-MM-dd",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM')) "yyyy-MM",
       count(distinct To_Timestamp(UPD_DATE,'yyyy')) "yyyy"
from TEST_CATEGORY_GROUPINGS