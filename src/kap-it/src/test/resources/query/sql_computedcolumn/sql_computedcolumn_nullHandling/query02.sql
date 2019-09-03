
select
count(ifnull(LSTG_FORMAT_NAME,'sorry, name is null')),
count(isnull(LSTG_FORMAT_NAME)),
sum(ifnull(PRICE,3.0)),
min(ifnull(PRICE,3.0)),
max(ifnull(PRICE,3.0)),
COUNT(DISTINCT IFNULL("LSTG_FORMAT_NAME", 'sorry, name is null')),
COUNT(DISTINCT ISNULL("LSTG_FORMAT_NAME"))
from TEST_KYLIN_FACT