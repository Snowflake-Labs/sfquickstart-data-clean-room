/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Data Onboarding
Create Date:        2022-12-02
Author:             B. Klein
Description:        Onboards additional data for the provider

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-12-02          B. Klein                            Initial Creation
2022-12-07          B. Klein                            Added reference_usage permission
*************************************************************************************************************/

/*
Find and replace the following values:

SOURCE_DATABASE - The database where the data to be shared lives
SOURCE_SCHEMA - The schema where the data to be shared lives
SOURCE_TABLE - The table where the data to be shared lives
SOURCE_COLUMN - A varchar column from the source table (used to anchor the RAP)
*/

use role securityadmin; --Security or account admin required to grant select permissions on data outside of the clean room

///////
/// GRANT SELECT PERMISSIONS ON TABLE OR VIEW
///////
grant usage on database SOURCE_DATABASE to role data_clean_room_role;
grant usage on schema SOURCE_DATABASE.SOURCE_SCHEMA to role data_clean_room_role;
grant select on SOURCE_DATABASE.SOURCE_SCHEMA.SOURCE_TABLE to role data_clean_room_role;
grant reference_usage on database SOURCE_DATABASE to share dcr_samp_app;

use role data_clean_room_role;
use warehouse app_wh;


//////////////////
// Protect provider data with Data Firewall
//////////////////

// create the view and schema to share the protected provider data
create or replace secure view dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw as select * from SOURCE_DATABASE.SOURCE_SCHEMA.SOURCE_TABLE;

// shields down
//alter view dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;

// shields up
alter view dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (SOURCE_COLUMN);

// test RAP
select * from dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw;  // should now return no rows

// test that the original table data is still accessible - note the tables are never shared
select * from dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw;  // should still return rows


//////
// share views to cleanroom
//////

grant select on dcr_samp_provider_db.cleanroom.SOURCE_TABLE_vw to share dcr_samp_app;
