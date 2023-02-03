/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Enable New Consumer
Create Date:        2022-06-22
Author:             J. Langseth, M. Rainey
Description:        Provider enabling an additional consumer for submitting requests. Depends
                    on the execution of script consumer_init.sql.


Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-06-22          J. Langseth, M. Rainey              Initial Creation
2022-06-30          M. Rainey                           Updated to use provider-specific schema for
                                                        requests table in support of multi-party.
2022-07-06          B. Klein                            Fixed a column reference bug with the budget reset
                                                        task that was failing silently.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
2022-08-23          M. Rainey                           Remove differential privacy
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;


//////////////

/*** CONSUMER RUNS CONSUMER_INIT.SQL ***/

/////////////

//////
/* enable additional consumer */
//////

create or replace database dcr_samp_CONSUMER_ACCT from share CONSUMER_ACCT.dcr_samp_requests_PROVIDER_ACCT;
create or replace stream dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT on table dcr_samp_CONSUMER_ACCT.PROVIDER_ACCT_schema.requests append_only = true show_initial_rows = true
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}';


CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_1
  SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh'
WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT')
AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');

CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_2 SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh' WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT') AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');
CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_3 SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh' WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT') AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');
CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_4 SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh' WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT') AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');
CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_5 SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh' WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT') AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');
CREATE OR REPLACE TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_6 SCHEDULE = '1 minute'  WAREHOUSE = 'app_wh' WHEN  SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT') AS call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');

// enable tasks 8 seconds apart to reduce consumer wait time for request approval

ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_1 RESUME;
call system$wait(8);
ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_2 RESUME;
call system$wait(8);
ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_3 RESUME;
call system$wait(8);
ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_4 RESUME;
call system$wait(8);
ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_5 RESUME;
call system$wait(8);
ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_6 RESUME;
show tasks in  dcr_samp_provider_db.admin;
