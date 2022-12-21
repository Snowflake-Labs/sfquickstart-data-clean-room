/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Template Addition
Create Date:        2022-12-08
Author:             B. Klein
Description:        Adds a custom provider template

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-12-08          B. Klein                            Initial Creation
*************************************************************************************************************/

/*
Find and replace the following values:

CONSUMER_ACCT - The account that will be using the template
NEW_TEMPLATE_NAME - The name of the template
NEW_TEMPLATE_TEXT - The actual JINJA template
NEW_DIMENSIONS - A pipe-separated list of optional dimensions
*/

use role data_clean_room_role;
use warehouse app_wh;


/////
// ADD TEMPLATE
/////

insert into dcr_samp_provider_db.templates.dcr_templates (party_account, template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT', 'NEW_TEMPLATE_NAME',
$$
NEW_TEMPLATE_TEXT
$$, NEW_DIMENSIONS, 'SQL');
