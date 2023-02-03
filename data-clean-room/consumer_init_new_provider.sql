/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Additional Provider Initialization
Create Date:        2022-06-30
Author:             J. Langseth, M. Rainey
Description:        Consumer side object and data initialization for additional provider
                    If running on SNOWCAT/SNOWCAT2, change _samp_ to something else everywhere

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-06-30          M. Rainey                           Initial Creation
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
2022-08-23          M. Rainey                           Remove differential privacy
2022-11-08          B. Klein                            Python GA
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;


///////
// CONSUMER INSTALLS THE CLEAN ROOM APP - PROVIDER_2
///////

// mount clean room app
create or replace database dcr_samp_app_two from share PROVIDER2_ACCT.dcr_samp_app;

// see templates from provider
select * from dcr_samp_app_two.cleanroom.templates;

// verify this simple query can NOT see providers data
select * from dcr_samp_app_two.cleanroom.provider_customers_vw;
// returns no rows but can see column names

// see the templates - provider2
select * from dcr_samp_app_two.cleanroom.templates;

create or replace schema dcr_samp_consumer.PROVIDER2_ACCT_schema;

create or replace table dcr_samp_consumer.PROVIDER2_ACCT_schema.requests (request_id varchar(1000),request variant, signature varchar(1000))
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}';
ALTER TABLE dcr_samp_consumer.PROVIDER2_ACCT_schema.requests SET CHANGE_TRACKING = TRUE;

///////
// creating Request stored proc
///////

create or replace procedure dcr_samp_consumer.PROVIDER2_ACCT_schema.request(in_template varchar(1000), in_params varchar(10000), request_id varchar(1000), at_timestamp VARCHAR(30))
    returns variant
    language javascript
    comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}'
    execute as owner as
    $$
    // get the provider account
    var result = snowflake.execute({ sqlText:  `select account_name from dcr_samp_app_two.cleanroom.provider_account ` });
    result.next();
    var provider_account = result.getColumnValue(1);

    // bring procedure parameters into JS variables, and remove unneeded special characters (low grade SQL injection defense)
    var in_template_name = IN_TEMPLATE.replace(/[^a-zA-Z0-9_]/g, "");
    var in_parameters = IN_PARAMS.replace(/[^a-zA-Z0-9_{}:"$\\s.\\,<>=\\+\\%\\-\\[\\]]/g, "");
    var request_id = REQUEST_ID;
    var multi_party_request = false;
    if(request_id && request_id != null ) {
      var multi_party_request = true;
    };
    //TODO check the below for SQL Injection?  Necessary?
    // set timestamp
    var at_timestamp = AT_TIMESTAMP;
    if (at_timestamp) {
      at_timestamp = "'"+at_timestamp+"'";
    } else {
      at_timestamp = "SYSDATE()::string";
    };

    // create the request JSON object with a SQL statements
    var request_sql = `
     with all_params as (
    with request_params as
     (
     select case when `+multi_party_request+` = true then '`+request_id+`' else replace(uuid_string(),'-','_') end as request_id,
     '`+in_template_name+`' as query_template,
      current_account() as requester_account,
      array_construct('`+provider_account+`') as provider_accounts,
     `+at_timestamp+` as request_ts
      ),
     query_params as
    (select parse_json('`+in_parameters+`') query_params
     ),
    settings as
     (
     select object_agg(setting_name, setting_value::variant) as settings from dcr_samp_consumer.local.user_settings
      ),
     app_instance_id as
      (
     select any_value(id) as app_instance_id from dcr_samp_consumer.util.instance
     ),
    query_params_full as
    (
     select request_params.*, parse_json(left(query_params::varchar,len(query_params::varchar)-1) ||
                                    ', "at_timestamp": "'||request_params.request_ts::varchar||'",' ||
                                    '"request_id": "'||request_params.request_id||'",' ||
                                     '"app_instance_id": "'||app_instance_id||'",' ||
                                    right(settings::varchar,len(settings::varchar::varchar)-1)) as request_params
     from query_params, settings, request_params, app_instance_id
     ) , proposed_query as (
             select dcr_samp_consumer.util.get_sql_jinja(
            (select template from dcr_samp_app_two.cleanroom.templates where template_name = rp.query_template), qpf.request_params) as proposed_query,
            sha2(proposed_query) as proposed_query_hash from query_params_full qpf, request_params rp)
     select rp.*, pq.*, f.request_params
        from query_params_full f, request_params rp, proposed_query pq )
     select object_construct(*) as request from all_params;`;

  var result = snowflake.execute({ sqlText: request_sql });
  result.next();
  var request = result.getColumnValue(1);

  // put request JSON into a temporary place so if it is approved we can use it later directly from SQL to avoid JS altering it
   var result = snowflake.execute({ sqlText: `create or replace table dcr_samp_consumer.PROVIDER2_ACCT_schema.request_temp as select REQUEST FROM      table(result_scan(last_query_id()));` });

  var signed_request ='';

 //insert the signed request
  var insert_sql =  `insert into dcr_samp_consumer.PROVIDER2_ACCT_schema.requests (request_id , request, signature )
  select (select any_value(request:REQUEST_ID) from PROVIDER2_ACCT_schema.request_temp),(select any_value(request) from PROVIDER2_ACCT_schema.request_temp), '`+signed_request+`' ; `;
  var r_stmt = snowflake.createStatement( { sqlText: insert_sql } );
  var result = r_stmt.execute();

  var result = snowflake.execute({ sqlText:`select any_value(request:REQUEST_ID) from PROVIDER2_ACCT_schema.request_temp` });
  result.next();
  var request_id = result.getColumnValue(1);

  return [ "Request Sent", request_id , request];

$$;

// share the request table to the provider
create or replace share dcr_samp_requests_PROVIDER2_ACCT;
grant usage on database dcr_samp_consumer to share dcr_samp_requests_PROVIDER2_ACCT;
grant usage on schema dcr_samp_consumer.PROVIDER2_ACCT_schema to share dcr_samp_requests_PROVIDER2_ACCT;
grant select on dcr_samp_consumer.PROVIDER2_ACCT_schema.requests to share dcr_samp_requests_PROVIDER2_ACCT;
alter share dcr_samp_requests_PROVIDER2_ACCT add accounts = PROVIDER2_ACCT;
