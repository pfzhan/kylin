--https://github.com/Kyligence/KAP/issues/6533
-- from KE query/sql_not_in_h2_sparder_function/query01.sql
-- commit 0bb8203

select initcap(lstg_format_name) a
from test_kylin_fact
group by lstg_format_name
order by a