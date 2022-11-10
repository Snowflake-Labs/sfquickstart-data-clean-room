/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Uninstall
Create Date:        2022-08-05
Author:             B. Klein
Description:        Removes data clean room from provider

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-08-05          B. Klein                            Initial Creation
*************************************************************************************************************/

use role data_clean_room_role;

//cleanup//
drop share if exists dcr_samp_app;
drop database if exists dcr_samp_provider_db;
drop database if exists dcr_samp_CONSUMER_ACCT;
