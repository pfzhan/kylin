SELECT * FROM KYLIN_SALES as KYLIN_SALES  
INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT 
INNER JOIN KYLIN_CATEGORY_GROUPINGS as KYLIN_CATEGORY_GROUPINGS 
    ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID 
INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID 
--INNER JOIN KYLIN_ACCOUNT as SELLER_ACCOUNT ON KYLIN_SALES.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID 
INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY 
--INNER JOIN KYLIN_COUNTRY as SELLER_COUNTRY ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY