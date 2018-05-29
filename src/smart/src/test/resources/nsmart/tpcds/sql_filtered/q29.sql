-- SQL q29.sql
select   
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,sum(ss_quantity)        as store_sales_quantity
    ,sum(sr_return_quantity) as store_returns_quantity
    ,sum(cs_quantity)        as catalog_sales_quantity
 from
    store_sales
   join store_returns on ss_customer_sk         = sr_customer_sk
      and ss_item_sk             = sr_item_sk
      and ss_ticket_number       = sr_ticket_number
   join catalog_sales on sr_customer_sk         = cs_bill_customer_sk
      and sr_item_sk             = cs_item_sk
   join date_dim             d1 on d1.d_date_sk           = ss_sold_date_sk
   join date_dim             d2 on sr_returned_date_sk    = d2.d_date_sk
   join date_dim             d3 on cs_sold_date_sk        = d3.d_date_sk
   join store on s_store_sk             = ss_store_sk
   join item on i_item_sk              = ss_item_sk
 where
     d1.d_moy               = 2 
 and d1.d_year              = 2000
 and d2.d_moy               between 2 and  2 + 3 
 and d2.d_year              = 2000
 and d3.d_year              in (2000,2000+1,2000+2)
 group by
    i_item_id
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 order by
    i_item_id 
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 limit 100