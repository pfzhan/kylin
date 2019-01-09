--https://github.com/Kyligence/KAP/issues/6520

select count(*)
from test_kylin_fact
cross join (select 1 as a) tmp
where tmp.a=1