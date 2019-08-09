select is_screen_on, count(1) as num from
(
select trans_id,
  case when TEST_ACCOUNT.ACCOUNT_ID >= 10000336 then 1
    else 2
    end as is_screen_on
from TEST_KYLIN_FACT
inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
)
group by is_screen_on