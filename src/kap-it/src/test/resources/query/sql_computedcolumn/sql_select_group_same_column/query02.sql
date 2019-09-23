select order_id+1, sum(seller_id+1)
from TEST_KYLIN_FACT
group by order_id