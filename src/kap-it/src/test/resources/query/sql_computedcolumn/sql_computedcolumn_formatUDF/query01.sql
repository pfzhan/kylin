

select count(TO_TIMESTAMP("UPD_DATE")) "NoFmt",
       count(To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm:ss')) "yyyy-MM-dd HH:mm:ss",
       count(To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm')) "yyyy-MM-dd HH:mm",
       count(To_Timestamp(UPD_DATE,'yyyy-MM-dd HH')) "yyyy-MM-dd HH",
       count(To_Timestamp(UPD_DATE,'yyyy-MM-dd')) "yyyy-MM-dd",
       count(To_Timestamp(UPD_DATE,'yyyy-MM')) "yyyy-MM",
       count(To_Timestamp(UPD_DATE,'yyyy')) "yyyy"
from TEST_CATEGORY_GROUPINGS