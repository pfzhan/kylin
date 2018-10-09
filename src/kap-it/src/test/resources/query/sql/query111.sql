select (case when "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" = 'Auction'
        then ( case when "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" = 'Auction' then LSTG_FORMAT_NAME else null end )
        else null end)
 from test_kylin_fact