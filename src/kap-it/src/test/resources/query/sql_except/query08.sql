select count(1), sum(c1) from (
(select sum(ITEM_COUNT+1) as c1, SELLER_ID as c2
from TEST_KYLIN_FACT
where LSTG_FORMAT_NAME='FP-GTC'
group by SELLER_ID
)
EXCEPT
(select sum(item_count + price) as c1, SELLER_ID as c2
from TEST_KYLIN_FACT
where LSTG_FORMAT_NAME='Others'
group by SELLER_ID
)
)
group by c2
