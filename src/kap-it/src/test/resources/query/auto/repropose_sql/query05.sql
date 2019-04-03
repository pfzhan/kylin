

select account_country
from test_account inner join test_country on test_account.account_country = test_country.country
order by account_country
limit 10