select sum(order_id+1)
from TEST_KYLIN_FACT
group by seller_id