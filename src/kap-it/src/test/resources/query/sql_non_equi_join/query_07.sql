select sum(price)
from (select SELLER_ID, price+1 as price, lstg_format_name, cal_dt from test_kylin_fact) test_kylin_fact
left join test_account
ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
 and test_kylin_fact.lstg_format_name='FP-GTC'
 and test_kylin_fact.cal_dt between '2013-05-01' and DATE '2013-08-01'
group by test_kylin_fact.cal_dt
