/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Consumer Data, Media & Advertising
Create Date:        2022-10-25
Author:             B. Klein
Description:        Cleanroom Consumer side data initialization for the Media & Adveritising use case

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-10-25          B. Klein                            Initial Creation
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;

// verify this simple query can NOT see providers data
select * from dcr_samp_app.cleanroom.provider_customers_vw;
// returns no rows but can see column names


///////
// CONSUMER_ACCT DATA SETUP
///////

create or replace schema dcr_samp_consumer.mydata;

// create sample customers with other features (~33% of these should join to the providers emails due to the uniform(1, 3, random()) in the email addresses)

create or replace table dcr_samp_consumer.mydata.customers 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}'
as
select 'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email,
 replace(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||uniform(1, 10, random()),' ','') as phone,
  case when uniform(1,10,random())<2 then 'CAT'
       when uniform(1,10,random())>=2 then 'DOG'
       when uniform(1,10,random())>=1 then 'BIRD'
  else 'NO_PETS' end as pets,
  round(20000+uniform(0,65000,random()),-2) as zip
  from table(generator(rowcount => 1000000))
  ;

create or replace table dcr_samp_consumer.mydata.conversions 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}'
as
select email, 'product_'||uniform(1,5,random()) as product,
('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as sls_date,
uniform(1,100,random())+uniform(1,100,random())/100 as sales_dlr
from dcr_samp_consumer.mydata.customers sample (10)
;

//select * from dcr_samp_consumer.mydata.customers;
//select * from dcr_samp_consumer.mydata.conversions;


///////
// CONSUMER_ACCT USER SETTINGS
///////

//create local settings table - provider1
create or replace schema dcr_samp_consumer.local;
create or replace table dcr_samp_consumer.local.user_settings (setting_name varchar(1000), setting_value varchar(1000))
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}';

//populate the local settings table - provider1
delete from dcr_samp_consumer.local.user_settings;
insert into dcr_samp_consumer.local.user_settings (setting_name, setting_value)
 VALUES ('app_data','dcr_samp_app'),
        ('app_two_data','dcr_samp_app_two'),
        ('app_three_data','dcr_samp_app_three'),
        ('consumer_db','dcr_samp_consumer'),
        ('consumer_schema','mydata'),
        ('consumer_table','customers'),
        ('consumer_join_field','email'),
        ('app_instance','dcr_samp_app'),
        ('app_two_instance','dcr_samp_app_two'),
        ('app_three_instance','dcr_samp_app_three'),
        ('consumer_email_field','email'),
        ('consumer_phone_field','phone'),
        ('consumer_customer_table','customers'),
        ('consumer_conversions_table','conversions'),
        ('consumer_requests_table','dcr_samp_consumer.PROVIDER_ACCT_schema.requests'),
        ('consumer_internal_join_field','email')
        ;
