-- issue #15124
select * from TEST_KYLIN_FACT
left join TEST_ACCOUNT
on TEST_KYLIN_FACT.LSTG_FORMAT_NAME = 'FP-GTC'
and TEST_KYLIN_FACT.seller_id = TEST_ACCOUNT.account_id;
