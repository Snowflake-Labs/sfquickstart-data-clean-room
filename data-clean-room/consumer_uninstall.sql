/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Consumer Uninstall
Create Date:        2022-08-05
Author:             B. Klein
Description:        Removes data clean room from consumer


Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-08-05          B. Klein                            Initial Creation
*************************************************************************************************************/

use role accountadmin;

//cleanup//
drop share if exists dcr_samp_requests;
drop database if exists dcr_samp_consumer;
drop database if exists dcr_samp_app;
