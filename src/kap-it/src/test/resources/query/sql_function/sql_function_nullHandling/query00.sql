-- https://github.com/Kyligence/KAP/issues/13615

select ifnull(LSTG_FORMAT_NAME,'sorry, name is null'),
       ifnull(TRANS_ID,0),
       ifnull(PRICE,0),
       ifnull(CAL_DT,CAL_DT),
       isnull(LSTG_FORMAT_NAME),
       isnull(TRANS_ID),
       isnull(PRICE),
       isnull(CAL_DT)
from TEST_KYLIN_FACT;