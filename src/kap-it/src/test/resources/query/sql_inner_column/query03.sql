select upper(lstg_format_name) as lstg_format_name, count(*) as cnt
from test_kylin_fact
where upper(lstg_format_name)='ABIN'
group by lstg_format_name