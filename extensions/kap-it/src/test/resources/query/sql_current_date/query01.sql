SELECT sum(price)  as sum_price
 FROM TEST_KYLIN_FACT 
 WHERE CAL_DT > cast(TIMESTAMPADD(Day, -1500, CURRENT_DATE) as DATE)
GROUP BY CAL_DT