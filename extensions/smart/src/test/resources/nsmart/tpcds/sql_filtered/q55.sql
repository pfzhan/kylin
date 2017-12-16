-- SQL q55.sql
select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from store_sales
 join date_dim on date_dim.d_date_sk = store_sales.ss_sold_date_sk
 join item on store_sales.ss_item_sk = item.i_item_sk
 where i_manager_id=36
 	and d_moy=12
 	and d_year=2001
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
limit 100