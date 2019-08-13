select cal_dt, sum(case when c1 > 100 then 100 else c1 end) from (
    select
    TEST_KYLIN_FACT.ITEM_COUNT * TEST_KYLIN_FACT.PRICE as c1 ,
    CAL_DT
    from
        TEST_KYLIN_FACT
    where
     CAL_DT  >   date'2013-12-30'
     )
 group by
 CAL_DT