select count(T.ACCOUNT_COUNTRY), ACCOUNT_SELLER_LEVEL
 from (
 select ACCOUNT_COUNTRY, ACCOUNT_SELLER_LEVEL
 from TEST_ACCOUNT
 order by ACCOUNT_COUNTRY desc
 ) as T
 inner join TEST_COUNTRY E
 on T.ACCOUNT_COUNTRY = E.COUNTRY
group by ACCOUNT_SELLER_LEVEL
