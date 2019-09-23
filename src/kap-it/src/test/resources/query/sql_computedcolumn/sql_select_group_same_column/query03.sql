select count(1), sum(price + item_count)
from TEST_KYLIN_FACT
where price + item_count > 1
group by price + item_count
having price + item_count > 2