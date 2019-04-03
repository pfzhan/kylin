
select lstg_format_name, sum(price)
from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id
inner join test_country on test_account.account_country = test_country.country
group by lstg_format_name
order by lstg_format_name
limit 10