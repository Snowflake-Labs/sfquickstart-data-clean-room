/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Enable Consumer
Create Date:        2022-02-09
Author:             J. Langseth, M. Rainey
Description:        Provider enabling the consumer for submitting requests. Depends
                    on the execution of script consumer_init.sql.

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-02-09          J. Langseth, M. Rainey              Initial Creation
2022-04-01          V. Malik                            v5.5 [Version without Native app , with JINJASQL
                                                        template & DP]
2022-04-13          M. Rainey                           Added SQL based templates & get_sql_js function.
                                                        Compare query from consumer side with query formed on
                                                        provider side.
2022-04-22          M. Rainey                           Allow templates without diff privacy sensitivity to process without rejection.
                                                        Updated test process requests code to run for v5.5.
                                                        Added per-consumer privacy budget reset task.
2022-06-30          M. Rainey                           Updated to use provider-specific schema for
                                                        requests table in support of multi-party.
                                                        Modified use of get_sql_js function to use cleanroom
                                                        schema.
2022-07-06          B. Klein                            Fixed column reference bug with refresh stored procedure
                                                        that was failing silently.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
2022-08-23          M. Rainey                           Remove differential privacy
2022-11-08          B. Klein                            Python GA
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;

/////
// REQUEST HANDLING FROM CONSUMER_ACCT
/////

// create a new mounted request database, stream, and task for each consumer

////////
/// DEMO RESET PROVIDER_ACCT SIDE
/// run this to reset the whole provider side for all consumers (for demo purposes)
////////

// mount request share from consumer
// NOTE show_initial_rows is not supposed to work across a share but it does and using here for ease of debugging

create or replace database dcr_samp_CONSUMER_ACCT from share CONSUMER_ACCT.dcr_samp_requests_PROVIDER_ACCT;
create or replace stream dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT on table dcr_samp_CONSUMER_ACCT.PROVIDER_ACCT_schema.requests append_only = true show_initial_rows = true
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}';
delete from dcr_samp_provider_db.admin.request_log;

// see request status and streams

// see requests
select * from dcr_samp_CONSUMER_ACCT.PROVIDER_ACCT_schema.requests;
// see request stream
select * from dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT;
// see if the stream has anything in it
select SYSTEM$STREAM_HAS_DATA('dcr_samp_provider_db.admin.request_stream_CONSUMER_ACCT');
// see processed request log
select * from dcr_samp_provider_db.admin.request_log;

////////
// REQUEST VALIDATION
////////

// validate the new stuff right from the stream into the validation table

//clean request log
delete from dcr_samp_provider_db.admin.request_log;

//process_requests modified for version 5.5 to add validation part on provider side

create or replace procedure dcr_samp_provider_db.admin.process_requests(party_account string)
returns string
language javascript
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“provider”}}'
execute as owner
as $$

var max_request_age_minutes = 5;
//pull request parameters from request stream

 var request_tbl_sql =`create or replace table dcr_samp_provider_db.admin.request_tmp as select REQUEST:REQUEST_ID as REQUEST_ID,
 replace(REQUEST:QUERY_TEMPLATE,'"') query_template,
 replace(REQUEST:REQUESTER_ACCOUNT,'"') requester_account
 ,replace(replace(replace(REQUEST:PROVIDER_ACCOUNTS,'"'),'[') ,']') provider_accounts
 ,REQUEST:REQUEST_TS request_ts, REQUEST as request
 ,REQUEST:REQUEST_PARAMS REQUEST_PARAMS
 ,REQUEST:REQUEST_PARAMS."dimensions" as select_fields
 ,REQUEST:REQUEST_PARAMS."where_clause" as where_fields
from dcr_samp_provider_db.admin.request_stream_`+PARTY_ACCOUNT+` `;

var request_tbl_sql_exec= snowflake.execute({sqlText: request_tbl_sql});
request_tbl_sql_exec.next();

  // check data in stream
  var result = snowflake.execute({ sqlText: `select count(1) from dcr_samp_provider_db.admin.request_tmp` });
  result.next();
  var count = result.getColumnValue(1);
   if( count == 0){
  return ['Stream empty : Request Input not recieved'];
        }

 // get template available fields
  var result = snowflake.execute({ sqlText: `select lower(dimensions) from dcr_samp_provider_db.templates.dcr_templates where party_account=(select any_value(requester_account) from dcr_samp_provider_db.admin.request_tmp) and template_name = (select any_value(query_template) from dcr_samp_provider_db.admin.request_tmp)` });
  result.next();
  var dimensions = result.getColumnValue(1);

// get request_id
  var result = snowflake.execute({ sqlText: `select REQUEST_ID,select_fields from dcr_samp_provider_db.admin.request_tmp` });
  result.next();
  var request_id = result.getColumnValue(1);
  var select_field = result.getColumnValue(2);

//Validation based on Input
    var unapproved_entry = ` insert into dcr_samp_provider_db.admin.request_log (party_account, request_id, request_ts, request, app_instance_id,query_hash,    template_name, epsilon, sensitivity, processed_ts, approved,error)
        with request as (
        select req.REQUEST_ID
        ,req.query_template query_template
        ,req.requester_account requester_account
        ,req.provider_accounts
        ,req.request_ts
        ,req.request
        ,req.REQUEST_PARAMS
        from dcr_samp_provider_db.admin.request_tmp req
          )
        select requester_account,REQUEST_ID,request_ts,request,NULL,NULL,query_template,NULL,NULL,SYSDATE(),:2,:1 from request;`;

  //1.TimeStamp check that request is not too old or from the future

     var result = snowflake.execute({ sqlText: `select request_ts from dcr_samp_provider_db.admin.request_tmp` });
     result.next();
     var request_ts = result.getColumnValue(1);

     var result = snowflake.execute({ sqlText: `select datediff('minute','`+request_ts+`'::TIMESTAMP,SYSDATE());` });
     result.next();
     var request_minutes_old = result.getColumnValue(1);

     var approved_flag='false';
    if (request_minutes_old > max_request_age_minutes) {
        var error_val = 'Not approved: request is '+request_minutes_old+' minutes old. The maxiumum request age is '+max_request_age_minutes+' minutes.';
        var result = snowflake.execute({ sqlText: unapproved_entry,binds:[error_val,approved_flag]});
        result.next();
        return [error_val];
        };

    if (request_minutes_old < 0) {
        var error_val = 'Not approved: request has a future timestamp.';
        var result = snowflake.execute({ sqlText: unapproved_entry,binds:[error_val,approved_flag]});
        result.next();
        return [error_val];

        };

 // verify list of available values
    if (select_field == null) {
        var dimensions_val_okay = 1;
    } else {
      // verify list of available values
        var dimensions_val_okay = 0;
        var dimensions_val = dimensions.replace(/\|/g,"','");

        var result = snowflake.execute({ sqlText: `with req_dim as
        (select select_fields as r_dim
        from dcr_samp_provider_db.admin.request_tmp)
        ,tem_dim as
        (select array_construct('`+dimensions_val+`') as t_dim)
        select case when array_size(array_intersection(r_dim,t_dim))=array_size(r_dim) then 1 else 0 end as res from req_dim,tem_dim`});
        result.next();
        var dimensions_val_okay = result.getColumnValue(1);

         if ( dimensions_val_okay == 0 ){
          var error_val = 'Not approved:List of dimensions passed does not match with available values :'+dimensions_val+' .';
          var result = snowflake.execute({ sqlText: unapproved_entry,binds:[error_val,approved_flag]});
          result.next();
          return [error_val];
       }
    }

//Validation Completed
var sql_text = `
insert into dcr_samp_provider_db.admin.request_log (party_account, request_id, request_ts, request, app_instance_id,  query_hash, template_name, epsilon, sensitivity, processed_ts, approved,error)
with request as (
 select req.REQUEST_ID
 ,req.query_template query_template
 ,req.requester_account requester_account
 ,req.provider_accounts
 ,req.request_ts
 ,req.request
 ,req.REQUEST_PARAMS
from dcr_samp_provider_db.admin.request_tmp req
              ) ,
query_params_full as (
 select parse_json(REQUEST:REQUEST_PARAMS) request_params
        from dcr_samp_provider_db.admin.request_tmp
       ),
proposed_query as (
 select dcr_samp_provider_db.templates.get_sql_jinja(
     (select any_value(template) from dcr_samp_provider_db.templates.dcr_templates where template_name = rp.query_template
       and party_account=rp.requester_account), qpf.request_params) as proposed_query,
       request:PROPOSED_QUERY_HASH::varchar as proposed_query_hash
      from query_params_full qpf, request rp
    ),
validate as (
  select 'pending' as status,
  request.REQUEST_ID::varchar as request_id,
  sha2(pq.PROPOSED_QUERY) = pq.proposed_query_hash as VALID_HASH,
  sha2(request.request) req_hash,
  request.REQUEST_PARAMS:app_instance_id::varchar as app_instance_id,
  request.query_template,
  pq.proposed_query proposed_query,
  request.provider_accounts,
  request.REQUEST_PARAMS
  from request , proposed_query pq
  where provider_accounts = current_account()
  )
        ,
validate_sql as (
  select request.query_template requested_template,
  request:PROPOSED_QUERY::varchar proposed_query,
  request.REQUEST_PARAMS request_params,
  request.REQUEST_ID request_id,
  t.template template,
  t.template_name template_name,
  dcr_samp_provider_db.templates.get_sql_jinja(template, request_params) as valid_sql,
  proposed_query = valid_sql as VALID_QUERY,
  sha2(valid_sql) as valid_sql_hash,
  t.dp_sensitivity sensitivity,
  //OBJECT_INSERT(request.request,'PROPOSED_QUERY',proposed_query ) upd_request,
  request.provider_accounts as provider_accounts
  from request ,proposed_query pq, dcr_samp_provider_db.templates.dcr_templates t
  where t.template_name = requested_template
  and request.provider_accounts = current_account()
  )

select DISTINCT r.requester_account,
       r.REQUEST_ID::varchar,
       r.REQUEST_TS::timestamp,
       request,
       v.app_instance_id,
       vs.VALID_SQL_HASH,
       vs.template_name,
       NULL,
       NULL,
       SYSDATE(),
       v.VALID_HASH AND vs.VALID_QUERY  as approved
        ,'None'
from request r, validate v, validate_sql vs
where r.REQUEST_ID = v.request_id and
      r.REQUEST_ID = vs.request_id and
      r.provider_accounts = CURRENT_ACCOUNT();`;
snowflake.execute({ sqlText: sql_text });


  // get proposed query
    var result = snowflake.execute({ sqlText: `select query_hash from dcr_samp_provider_db.admin.request_log where REQUEST_ID ='`+request_id+`'; ` });
    result.next();
    var proposed_query_hash = result.getColumnValue(1);

  return ['approved:Done'];
$$;

// process approvals manually one-time, replace CONSUMER_ACCT with the consumer account name

call dcr_samp_provider_db.admin.process_requests('CONSUMER_ACCT');

////////
/// ENABLE EACH CONSUMER_ACCT
/// Replace all instances of CONSUMER_ACCT below with the name of the consumer account
////////

// set up a fleet of tasks to automatically process consumer requests

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

// use these to later pause the clean room request approval tasks, if needed

//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_1 SUSPEND;
//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_2 SUSPEND;
//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_3 SUSPEND;
//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_4 SUSPEND;
//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_5 SUSPEND;
//ALTER TASK dcr_samp_provider_db.admin.process_requests_CONSUMER_ACCT_6 SUSPEND;




// use this if needed to debug request failures, it provides visiblity to the results of each validation
// request_tmp table must be a permanent or transient table in the process_requests procedure.
/*
with request as (
 select req.REQUEST_ID
 ,req.query_template query_template
 ,req.requester_account requester_account
 ,req.provider_accounts
 ,req.request_ts
 ,req.request
 ,req.REQUEST_PARAMS
from dcr_samp_provider_db.admin.request_tmp req
              ) ,
query_params_full as (
 select parse_json(REQUEST:REQUEST_PARAMS) request_params
        from dcr_samp_provider_db.admin.request_tmp
       ),
proposed_query as (
 select dcr_samp_provider_db.templates.get_sql_jinja(
     (select any_value(template) from dcr_samp_provider_db.templates.dcr_templates where template_name = rp.query_template
       and party_account=rp.requester_account), qpf.request_params) as proposed_query,
       request:PROPOSED_QUERY_HASH::varchar as proposed_query_hash
      from query_params_full qpf, request rp
    ),
validate as (
  select 'pending' as status,
  request.REQUEST_ID::varchar as request_id,
  sha2(pq.PROPOSED_QUERY) = pq.proposed_query_hash as VALID_HASH,
  sha2(request.request) req_hash,
  request.REQUEST_PARAMS:app_instance_id::varchar as app_instance_id,
  request.query_template,
  pq.proposed_query proposed_query,
  request.provider_accounts,
  request.REQUEST_PARAMS
  from request , proposed_query pq
  where provider_accounts = current_account()
  )
        ,
validate_sql as (
  select request.query_template requested_template,
  request:PROPOSED_QUERY::varchar proposed_query,
  request.REQUEST_PARAMS request_params,
  request.REQUEST_ID request_id,
  t.template template,
  t.template_name template_name,
  dcr_samp_provider_db.templates.get_sql_jinja(template, request_params) as valid_sql,
  proposed_query = valid_sql as VALID_QUERY,
  sha2(valid_sql) as valid_sql_hash,
  t.dp_sensitivity sensitivity,
  //OBJECT_INSERT(request.request,'PROPOSED_QUERY',proposed_query ) upd_request,
  request.provider_accounts as provider_accounts
  from request ,proposed_query pq, dcr_samp_provider_db.templates.dcr_templates t
  where t.template_name = requested_template
  and request.provider_accounts = current_account()
  )

select DISTINCT r.requester_account,
       r.REQUEST_ID::varchar,
       r.REQUEST_TS::timestamp,
       request,
       v.app_instance_id,
       vs.VALID_SQL_HASH,
       vs.template_name,
       vs.sensitivity,
       SYSDATE(),
       v.VALID_HASH AND vs.VALID_QUERY  as approved
        ,'None'
from request r, validate v, validate_sql vs
where r.REQUEST_ID = v.request_id and
      r.REQUEST_ID = vs.request_id and
      r.provider_accounts = CURRENT_ACCOUNT();
*/
