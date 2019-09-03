-- https://github.com/Kyligence/KAP/issues/13615

select count(ifnull(LSTG_FORMAT_NAME,'sorry, name is null')),
       count(ifnull(TRANS_ID,0)),
       count(ifnull(PRICE,0)),
       count(ifnull(CAL_DT,CAL_DT)),
       count(isnull(LSTG_FORMAT_NAME)),
       count(isnull(TRANS_ID)),
       count(isnull(PRICE)),
       count(isnull(CAL_DT))
from TEST_KYLIN_FACT;