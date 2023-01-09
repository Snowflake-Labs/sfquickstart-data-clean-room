/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Consumer Request
Create Date:        2022-02-09
Author:             J. Langseth, M. Rainey
Description:        Consumer side data clean room request process


Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-02-09          J. Langseth, M. Rainey              Initial Creation
2022-04-13          M. Rainey                           Added call using non-Jinja template
2022-06-30          M. Rainey                           Added multi-party request example.
2022-07-06          B. Klein                            Added Jinja multi-party example.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
2022-08-23          M. Rainey                           Remove differential privacy
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;

///////
// CONSUMER_ACCT MAKES A REQUEST
///////

//DEMO RESET CONSUMER_ACCT SIDE as needed
//create or replace database dcr_samp_app from share PROVIDER_ACCT.dcr_samp_app;
//delete from dcr_samp_consumer.PROVIDER_ACCT_schema.requests;
// -> Now, reset provider side, otherwise provider will block requests if there is a previous request from an older app instance ID

// see schema
select * from dcr_samp_app.cleanroom.provider_customers_vw;

// see the templates
select * from dcr_samp_app.cleanroom.templates;

// see previous requests, if any
select * from dcr_samp_consumer.PROVIDER_ACCT_schema.requests;

/////
// JINJA template requests
/////

// REQUEST 1, OVERLAP COUNT JOIN ON EMAIL

// clean room app creates and signs the request
// try with combinations 'c.zip', 'c.pets', 'p.status', 'p.age_band'
call dcr_samp_consumer.PROVIDER_ACCT_schema.request('customer_overlap',
        object_construct(
            'dimensions',array_construct('c.zip', 'c.pets', 'p.age_band')
            )::varchar, NULL, NULL);

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar from dcr_samp_app.cleanroom.provider_log;

// run query (paste in the query text shown in the results of the above select .. from provider_log, and run it)




// REQUEST 2, OVERLAP COUNT BOOLEAN OR JOIN ON EMAIL AND PHONE WITH WHERE CLAUSE

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER_ACCT_schema.request('customer_overlap_waterfall',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band'),
            'where_clause',' c.PETS <> $$BIRD$$ '
            )::varchar, NULL, NULL);

// see requests
select * from dcr_samp_consumer.PROVIDER_ACCT_schema.requests;

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar from dcr_samp_app.cleanroom.provider_log;

// run query (paste in the query text shown in the results of the above select .. from provider_log, and run it)




// REQUEST 3, CAMPAIGN CONVERSION

call dcr_samp_consumer.PROVIDER_ACCT_schema.request('campaign_conversion',
        object_construct(
            'dimensions',array_construct( 'c_conv.product', 'p_exp.campaign' ),
            'where_clause','c_conv.sls_date >= p_exp.exp_date'
            )::varchar, NULL, NULL);

// see requests
select * from dcr_samp_consumer.PROVIDER_ACCT_schema.requests;

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar from dcr_samp_app.cleanroom.provider_log;

// run query (paste in the query text shown in the results of the above select .. from provider_log, and run it)




// REQUEST 4 - multiparty - provider 1, CUSTOMER OVERLAP COUNT JOIN ON EMAIL
// see the templates
select * from dcr_samp_app.cleanroom.templates;
select * from dcr_samp_app_two.cleanroom.templates;

// NOTE - the timestamp parameter must be in UTC to enable timezone compatibility across accounts
set ts = SYSDATE();

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER1_schema.request('customer_overlap_multiparty',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band')
            )::varchar, NULL, $ts);

// if multi-party, get request ID for use in second provider request
set request_id1 = (select request_id from dcr_samp_consumer.PROVIDER1_schema.requests where request:REQUEST_PARAMS.at_timestamp::varchar=$ts);

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar, * from dcr_samp_app.cleanroom.provider_log order by request_ts desc;


// REQUEST 4 - multiparty - provider 2, CUSTOMER OVERLAP COUNT JOIN ON EMAIL
// pass in request id from provider 1 request

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER2_schema.request('customer_overlap_multiparty',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band')
            )::varchar, $request_id1, $ts);


// see requests from provider 2
select * from dcr_samp_consumer.PROVIDER2_schema.requests;

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar, * from dcr_samp_app_two.cleanroom.provider_log order by request_ts desc;

// run query (paste in the query text shown in the results of the above select .. from provider_log, and run it




// REQUEST 5 - multiparty - provider 1, SUBSCRIBER OVERLAP COUNT JOIN ON EMAIL
// see the templates
select * from dcr_samp_app.cleanroom.templates;
select * from dcr_samp_app_two.cleanroom.templates;
select * from dcr_samp_app_three.cleanroom.templates;

// NOTE - the timestamp parameter must be in UTC to enable timezone compatibility across accounts
set ts = SYSDATE();

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER1_schema.request('customer_overlap_multiparty_subscribers',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band')
            )::varchar, NULL, $ts);

// if multi-party, get request ID for use in second provider request
set request_id1 = (select request_id from dcr_samp_consumer.PROVIDER1_schema.requests where request:REQUEST_PARAMS.at_timestamp::varchar=$ts);

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar, * from dcr_samp_app.cleanroom.provider_log order by request_ts desc;


// REQUEST 5 - multiparty - provider 2, SUBSCRIBER OVERLAP COUNT JOIN ON EMAIL
// pass in request id from provider 1 request

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER2_schema.request('customer_overlap_multiparty_subscribers',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band')
            )::varchar, $request_id1, $ts);


// if multi-party, get request ID for use in second provider request
set request_id2 = (select request_id from dcr_samp_consumer.PROVIDER2_schema.requests where request:REQUEST_PARAMS.at_timestamp::varchar=$ts);

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar, * from dcr_samp_app_two.cleanroom.provider_log order by request_ts desc;


// REQUEST 5 - multiparty - provider 3, SUBSCRIBER OVERLAP COUNT JOIN ON EMAIL
// pass in request id from provider 2 request

// clean room app creates and signs the request
call dcr_samp_consumer.PROVIDER3_schema.request('customer_overlap_multiparty_subscribers',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band')
            )::varchar, $request_id2, $ts);


// see requests from provider 2
select * from dcr_samp_consumer.PROVIDER3_schema.requests;

// wait 5-10 seconds for this query to show the request as approved
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar, * from dcr_samp_app_THREE.cleanroom.provider_log order by request_ts desc;

// run query (paste in the query text shown in the results of the above select .. from provider_log, and run it

