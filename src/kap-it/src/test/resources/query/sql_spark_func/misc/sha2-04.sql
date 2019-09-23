select count(distinct  sha2(lstg_format_name, 256) )
from TEST_KYLIN_FACT
group by lstg_format_name