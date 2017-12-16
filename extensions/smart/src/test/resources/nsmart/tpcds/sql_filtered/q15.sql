-- SQL q15.sql
select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     join customer on catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     join customer_address on customer.c_current_addr_sk = customer_address.ca_address_sk 
     join date_dim on catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 where ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
        or customer_address.ca_state in ('CA','WA','GA')
        or catalog_sales.cs_sales_price > 500)
  and date_dim.d_qoy = 2 and date_dim.d_year = 2000
 group by ca_zip
 order by ca_zip
 limit 100