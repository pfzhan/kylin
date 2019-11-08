select * from (
(select *
from TEST_ACCOUNT
where ACCOUNT_COUNTRY='CN')
EXCEPT ALL
(select *
from TEST_ACCOUNT
where ACCOUNT_COUNTRY='FR')
EXCEPT
(select *
from TEST_ACCOUNT
where ACCOUNT_COUNTRY='GB')
)
