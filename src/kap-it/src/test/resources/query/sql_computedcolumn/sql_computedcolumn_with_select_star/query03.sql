select sum(fact.CC_AUTO_1)
from (
select * from TEST_KYLIN_FACT
) fact