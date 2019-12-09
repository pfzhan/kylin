select
lag(price,1) OVER ()  lm_active
from
test_kylin_fact
limit 20