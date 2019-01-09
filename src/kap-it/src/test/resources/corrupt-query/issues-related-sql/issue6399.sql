--https://github.com/Kyligence/KAP/issues/6399

select concat(cast(round((cast (sum(seller_id) as decimal) / (cast (sum(SELLER_ID)*1000000 as decimal)))*100,2) as varchar ),'%') papapa from test_kylin_fact