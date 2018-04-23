select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
   sum(ss_ext_sales_price) ext_price
 from store_sales
 join date_dim on date_dim.d_date_sk = store_sales.ss_sold_date_sk
 join item on store_sales.ss_item_sk = item.i_item_sk
 join customer on store_sales.ss_customer_sk = customer.c_customer_sk
 join customer_address on customer.c_current_addr_sk = customer_address.ca_address_sk
 join store on store_sales.ss_store_sk = store.s_store_sk
 where i_manager_id=7
   and d_moy=11
   and d_year=1999
   and substring(ca_zip,1,5) <> substring(s_zip,1,5) 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 ;