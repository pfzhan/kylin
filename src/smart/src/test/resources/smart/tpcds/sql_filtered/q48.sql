-- SQL q48.sql

select sum (ss_quantity)
 from store_sales
 join store on store.s_store_sk = store_sales.ss_store_sk
 join customer_demographics on customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
 join customer_address on store_sales.ss_addr_sk = customer_address.ca_address_sk
 join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk 
 where d_year = 1998
 and  
 (
  (
   cd_marital_status = 'M'
   and 
   cd_education_status = '4 yr Degree'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
   cd_marital_status = 'M'
   and 
   cd_education_status = '4 yr Degree'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
   cd_marital_status = 'M'
   and 
   cd_education_status = '4 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ca_country = 'United States'
  and
  ca_state in ('KY', 'GA', 'NM')
  and ss_net_profit between 0 and 2000  
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('MT', 'OR', 'IN')
  and ss_net_profit between 150 and 3000 
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('WI', 'MO', 'WV')
  and ss_net_profit between 50 and 25000 
  )
 )