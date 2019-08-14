select OTBL.PRICE, ITBL.C1
from (
    select SELLER_ID, cast(PRICE+1.0 as double) PRICE, CAL_DT from test_kylin_fact
) as OTBL
left join (
    select ACCOUNT_ID, trim(ACCOUNT_COUNTRY) as C1
    from TEST_ACCOUNT
) as ITBL
on OTBL.SELLER_ID < ITBL.ACCOUNT_ID
order by OTBL.PRICE, ITBL.C1 asc
LIMIT 10
