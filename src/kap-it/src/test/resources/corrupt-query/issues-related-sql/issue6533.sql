--https://github.com/Kyligence/KAP/issues/6533

select initcap(lstg_format_name) a
from test_kylin_fact
group by lstg_format_name
order by a