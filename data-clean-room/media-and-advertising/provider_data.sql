/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Data, Media & Advertising
Create Date:        2022-10-24
Author:             B. Klein
Description:        Provider demo data intialization for the Media & Adveritising use case

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-10-24          B. Klein                            Initial Creation
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/


use role data_clean_room_role;
use warehouse app_wh;


///////
/// CREATE PROVIDER_ACCT DATA
///////

// generate sample customers with emails and features
create or replace table dcr_samp_provider_db.shared_schema.customers comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' as
select 'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email,
 replace(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||uniform(1, 10, random()),' ','') as phone,
  case when uniform(1,10,random())>3 then 'MEMBER'
       when uniform(1,10,random())>5 then 'SILVER'
       when uniform(1,10,random())>7 then 'GOLD'
else 'PLATINUM' end as status,
round(18+uniform(0,10,random())+uniform(0,50,random()),-1)+5*uniform(0,1,random()) as age_band
from table(generator(rowcount => 1000000))
;

create or replace table dcr_samp_provider_db.shared_schema.exposures comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' as
select email, 'campaign_'||uniform(1,3,random()) as campaign,
  case when uniform(1,10,random())>3 then 'STREAMING'
       when uniform(1,10,random())>5 then 'MOBILE'
       when uniform(1,10,random())>7 then 'LINEAR'
else 'DISPLAY' end as device_type,
('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as exp_date,
uniform(1,60,random()) as sec_view,
uniform(0,2,random())+uniform(0,99,random())/100 as exp_cost
from dcr_samp_provider_db.shared_schema.customers sample (20)
;

// generate subscription information for users
create or replace table dcr_samp_provider_db.shared_schema.subscriptions comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' as
select
        'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email
    ,   uniform(0,1,random()) as is_subscribed
from table(generator(rowcount => 1000000))
;

//select * from dcr_samp_provider_db.shared_schema.customers;
//select * from dcr_samp_provider_db.shared_schema.exposures;
//select * from dcr_samp_provider_db.shared_schema.subscriptions;


//////////////////
// Protect providers base table with Data Firewall
//////////////////

// see that customer table is not yet protected with a data firewall
select * from dcr_samp_provider_db.shared_schema.customers;

// shields down
//alter view dcr_samp_provider_db.cleanroom.provider_customers_vw drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;
//alter view dcr_samp_provider_db.cleanroom.provider_exposures_vw drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;
//alter view dcr_samp_provider_db.cleanroom.provider_subscriptions_vw drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;

// create the view and schema to share the protected provider data
create or replace secure view dcr_samp_provider_db.cleanroom.provider_customers_vw 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' 
as select * from  dcr_samp_provider_db.shared_schema.customers;
create or replace secure view dcr_samp_provider_db.cleanroom.provider_exposures_vw 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' 
as select * from dcr_samp_provider_db.shared_schema.exposures;
create or replace secure view dcr_samp_provider_db.cleanroom.provider_subscriptions_vw 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}' 
as select * from dcr_samp_provider_db.shared_schema.subscriptions;

// shields up
alter view dcr_samp_provider_db.cleanroom.provider_customers_vw add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);
alter view dcr_samp_provider_db.cleanroom.provider_exposures_vw add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);
alter view dcr_samp_provider_db.cleanroom.provider_subscriptions_vw add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);

// test RAP
select * from dcr_samp_provider_db.cleanroom.provider_customers_vw;  // should now return no rows
select * from dcr_samp_provider_db.cleanroom.provider_exposures_vw;  // should now return no rows
select * from dcr_samp_provider_db.cleanroom.provider_subscriptions_vw;  // should now return no rows

// test that the original table data is still accessible - note the tables are never shared
select * from dcr_samp_provider_db.shared_schema.customers;  // should still return rows
select * from dcr_samp_provider_db.shared_schema.exposures;  // should still return rows
select * from dcr_samp_provider_db.shared_schema.subscriptions;  // should still return rows


//////
// share views to cleanroom
//////

grant select on dcr_samp_provider_db.cleanroom.provider_customers_vw to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_exposures_vw to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_subscriptions_vw to share dcr_samp_app;
