--https://github.com/Kyligence/KAP/issues/14305

select count(split_part(LSTG_FORMAT_NAME,'I',1))
from test_kylin_fact