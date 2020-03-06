select * from "DEFAULT".TEST_ACCOUNT where ACCOUNT_ID=
(select cast(ACCOUNT_COUNTRY as bigint) from  "DEFAULT".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU')
