-- KAP#16751 duplicated columns
with fact as
(
select seller_id as seller_id from test_kylin_fact
group by seller_id
),
account as
(
select account_id, account_id as user_id, account_country
from test_account
)
select account_id as account_id, user_id as user_id
from fact inner join account
on seller_id = user_id
and seller_id = account_id