-- from kap-it/src/test/resources/query/sql_not_in_h2_sparder_function/query01.sql \
-- query/sql_sparder_function/query02.sql
select INITCAP(LSTG_FORMAT_NAME) a
from TEST_KYLIN_FACT
group by LSTG_FORMAT_NAME
order by a