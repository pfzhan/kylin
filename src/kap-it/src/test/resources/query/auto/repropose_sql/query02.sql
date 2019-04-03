
select lstg_format_name, seller_id, sum(price)
from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id
group by lstg_format_name, seller_id
order by lstg_format_name, seller_id
limit 10