select count(distinct base64(lstg_format_name) )
from TEST_KYLIN_FACT
group by lstg_format_name