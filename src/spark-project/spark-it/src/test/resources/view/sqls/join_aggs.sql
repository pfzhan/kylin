--expected_model_hit=orders_model,orders_model,orders_model;
SELECT * FROM (SELECT (SELECT COUNT(DISTINCT O_TOTALPRICE) AS O_TOTALPRICE FROM TPCH.ORDERS_MODEL) AS O_TOTALPRICE
FROM TPCH.ORDERS_MODEL
GROUP BY O_TOTALPRICE) T1 INNER JOIN (SELECT O_TOTALPRICE FROM TPCH.ORDERS_MODEL) T2 ON T1.O_TOTALPRICE = T2.O_TOTALPRICE
