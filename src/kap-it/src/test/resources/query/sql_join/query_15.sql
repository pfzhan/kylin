select sum(ACCOUNT_SELLER_LEVEL + item_count)
from TEST_ACCOUNT inner JOIN
(
select ITEM_COUNT item_count, seller_id seller_id from TEST_KYLIN_FACT
union
select LSTG_SITE_ID item_count, seller_id seller_id from TEST_KYLIN_FACT
) FACT
on TEST_ACCOUNT.account_id = FACT.seller_id