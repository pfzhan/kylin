select count(distinct split_part(LSTG_FORMAT_NAME,'I',1))
from test_kylin_fact