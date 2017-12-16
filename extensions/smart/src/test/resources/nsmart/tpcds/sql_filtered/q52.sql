-- SQL q52.sql
select  dt.d_year
 	,item.i_brand_id brand_id
 	,item.i_brand brand
 	,sum(ss_ext_sales_price) ext_price
 from store_sales
 join date_dim dt on dt.d_date_sk = store_sales.ss_sold_date_sk
 join item on store_sales.ss_item_sk = item.i_item_sk
 where item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=1998
	 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 order by dt.d_year
 	,ext_price desc
 	,brand_id
limit 100