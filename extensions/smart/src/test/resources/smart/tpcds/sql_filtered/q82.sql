-- SQL q82.sql
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from store_sales
 join item on ss_item_sk = i_item_sk
 join inventory on inv_item_sk = i_item_sk
 join date_dim on d_date_sk=inv_date_sk
 where i_current_price between 30 and 30+30
 and d_date between '2002-05-30' and '2002-07-30'
 and i_manufact_id in (437,129,727,663)
 and inv_quantity_on_hand between 100 and 500
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100