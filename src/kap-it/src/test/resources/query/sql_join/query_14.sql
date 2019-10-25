select sum(ACCOUNT_SELLER_LEVEL + item_count)
from TEST_ACCOUNT inner JOIN
(
select count(1), ITEM_COUNT item_count, seller_id seller_id from TEST_KYLIN_FACT group by item_count, seller_id
) FACT
on TEST_ACCOUNT.account_id = FACT.seller_id