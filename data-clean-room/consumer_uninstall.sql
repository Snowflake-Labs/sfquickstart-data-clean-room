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

use role data_clean_room_role;

//cleanup//
show shares;
execute immediate $$
declare
  res resultset default (select $3 as name from table(result_scan(last_query_id())) where $4 = 'DCR_SAMP_CONSUMER');
  c1 cursor for res;
  share_var string;
begin
  open c1;
  for record in c1 do
    share_var:=record.name;
    execute immediate 'drop share if exists '|| :share_var;
  end for;
  return 'Shares deleted';
end;
$$;
drop database if exists dcr_samp_consumer;
drop database if exists dcr_samp_app;
