-- https://github.com/Kyligence/KAP/issues/13613

select length(substr(initcapb(lstg_format_name),2)) a
from test_kylin_fact