-- #6395 magine L2
WITH
    A   AS  (
        SELECT
         TRANS_ID, ORDER_ID, LSTG_FORMAT_NAME
        FROM
            TEST_KYLIN_FACT
    )
,   B   AS  (
        SELECT
         TRANS_ID, ORDER_ID, SELLER_ID
        FROM
            TEST_KYLIN_FACT
    )

SELECT
  A.TRANS_ID, B.ORDER_ID
FROM
     A
LEFT JOIN
    B
ON
    A.TRANS_ID  =   B.TRANS_ID