-- SQL q37.sql
-- start query 37 in stream 0 using template query37.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from inventory
    join item on inv_item_sk = i_item_sk
    join date_dim on d_date_sk=inv_date_sk
    join catalog_sales on cs_item_sk = i_item_sk
 where 
     i_current_price between 19 and 19 + 30
     and d_date between cast('2000-03-27' as timestamp) and cast('2000-05-26' as timestamp)
     and i_manufact_id in (874,844,819,868)
     and inv_quantity_on_hand between 100 and 500
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100