select ACCOUNT_ID
from TEST_ACCOUNT
where ACCOUNT_ID = (select ACCOUNT_ID
                    from TEST_ACCOUNT
                    where ACCOUNT_ID = 10000000)