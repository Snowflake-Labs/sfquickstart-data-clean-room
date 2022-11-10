/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Add Consumer to Share
Create Date:        2022-06-22
Author:             J. Langseth, M. Rainey
Description:        Add a new Consumer account to the Provider shares


Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-06-22          J. Langseth, M. Rainey              Initial Creation
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
*************************************************************************************************************/

use role data_clean_room_role;
use warehouse app_wh;


/////////////

/*** PROVIDER RUNS PROVIDER_TEMPLATES.SQL ***/

////////////


// add new consumer account to shares
alter share dcr_samp_app add accounts = CONSUMER_ACCT;
