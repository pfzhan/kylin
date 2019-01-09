

select lstg_format_name
from test_kylin_fact
where EXISTS
(select price from test_kylin_fact where cal_dt between '2012-01-01' and '2012-02-01');