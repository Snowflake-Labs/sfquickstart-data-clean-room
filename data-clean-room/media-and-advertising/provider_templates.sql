/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Templates, Media & Advertising
Create Date:        2022-02-09
Author:             J. Langseth, M. Rainey
Description:        Provider Jinja templates

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-02-09          J. Langseth, M. Rainey              Initial Creation
2022-04-01          V. Malik                            v5.5 [added party_account column in template table]
2022-04-13          M. Rainey                           Added non-Jinja template
2022-06-30          M. Rainey                           Added non-Jinja multi-party template.
2022-07-06          B. Klein                            Added Jinja multi-party template.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_  and added template_type to
                                                        templates to better support upgrades.
2022-08-05          B. Klein                            Separating js and Jinja for to help with DCR Assistant.
2022-08-23          M. Rainey                           Remove differential privacy
2022-08-31          B. Klein                            Renamed back to provider_templates now that we have
                                                        the javascript jinja-like parser
2022-11-08          B. Klein                            Python GA
*************************************************************************************************************/


use role data_clean_room_role;
use warehouse app_wh;


/////
// PROVIDER_ACCT TEMPLATES
/////

// if you want to edit the templates, run this then re-insert them
// delete from dcr_samp_provider_db.templates.dcr_templates;

//Change CONSUMER_ACCT with consumer account name and adjust query template as per requirement

insert into dcr_samp_provider_db.templates.dcr_templates (party_account,template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT','customer_overlap',
$$
select
    {% if dimensions %}
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , {% endif %}
    count(distinct p.email) as overlap
from
    {{ app_data | sqlsafe }}.cleanroom.provider_customers_vw p,
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c
where
    c.{{ consumer_join_field | sqlsafe }} = p.email
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
    {% if  where_clause  %}
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
{% if dimensions %}
    group by identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    {% endif %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc;
$$,'c.pets|c.zip|p.status|p.age_band', 'SQL');

insert into dcr_samp_provider_db.templates.dcr_templates (party_account,template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT','customer_overlap_multiparty',
$$
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as overlap
from
    {{ app_data | sqlsafe }}.cleanroom.provider_customers_vw p,
    {{ app_two_data | sqlsafe }}.cleanroom.provider_customers_vw p2,
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c
where
    c.{{ consumer_join_field | sqlsafe }} = p.email
    and c.{{ consumer_join_field | sqlsafe }} = p2.email
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
    {% if  where_clause  %}
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc;
$$,'c.pets|c.zip|p.status|p.age_band|p2.status|p2.age_band', 'SQL');


insert into dcr_samp_provider_db.templates.dcr_templates (party_account,template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT','customer_overlap_waterfall',
$$
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as overlap
from
    {{ app_data | sqlsafe }}.cleanroom.provider_customers_vw p
join    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c
on  (
      c.{{ consumer_email_field | sqlsafe }} = p.email
      or c.{{ consumer_phone_field | sqlsafe }} = p.phone
    )
    {% if  where_clause  %}
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc;
$$, 'c.pets|c.zip|p.status|p.age_band', 'SQL');

insert into dcr_samp_provider_db.templates.dcr_templates (party_account,template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT','campaign_conversion',
$$
with actual_result as
(
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as conversion_count
from
    {{ app_data | sqlsafe }}.cleanroom.provider_customers_vw p,
    {{ app_data | sqlsafe }}.cleanroom.provider_exposures_vw p_exp,
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_customer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c
join
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_conversions_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c_conv
        on c.{{ consumer_internal_join_field | sqlsafe }} = c_conv.{{ consumer_internal_join_field | sqlsafe }}
where
    (
      c.{{ consumer_email_field | sqlsafe }} = p.email
    )
    and p.email = p_exp.email
    {% if  where_clause  %}
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_customer_table| sqlsafe }}') and table_type = 'BASE TABLE')
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_conversions_table| sqlsafe }}') and table_type = 'BASE TABLE')
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc)
{% set d = dimensions[0].split('.') %}
select identifier({{ d[1] }})
    {% for dim in dimensions[1:] %}
    {% set d = dim.split('.') %} , identifier({{ d[1]  }})
    {% endfor %}
    , conversion_count as bought_after_exposure
    from actual_result;
$$, 'c.pets|c.zip|p.status|p.age_band|c_conv.product|p_exp.campaign|p_exp.device_type', 'SQL');


// multi-party Jinja, gets overlap that is subscribed (e.g., where 3rd party is a marketing platform)

insert into dcr_samp_provider_db.templates.dcr_templates (party_account,template_name, template, dimensions, template_type)
values ('CONSUMER_ACCT','customer_overlap_multiparty_subscribers',
$$
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as overlap
from
    {{ app_data | sqlsafe }}.cleanroom.provider_customers_vw p,
    {{ app_two_data | sqlsafe }}.cleanroom.provider_customers_vw p2,
    {{ app_three_data | sqlsafe }}.cleanroom.provider_subscriptions_vw p3,
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_ntz) c
where
    c.{{ consumer_join_field | sqlsafe }} = p.email
    and c.{{ consumer_join_field | sqlsafe }} = p2.email
    and c.{{ consumer_join_field | sqlsafe }} = p3.email and p3.is_subscribed = true
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
    {% if  where_clause  %}
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc;
$$,'c.pets|c.zip|c.high_value|p.status|p.age_band|p.region_code|p2.status|p2.age_band|p2.region_code|p3.status|p3.age_band|p3.region_code', 'SQL');


// see templates
// select * from dcr_samp_provider_db.templates.dcr_templates;

/*
// test template and sql renderer (note resulting sql will not run in this account)
select dcr_samp_provider_db.templates.get_sql_jinja(
  (select template from dcr_samp_provider_db.templates.dcr_templates where template_name = 'campaign_conversion'),
   object_construct(
            'dimensions',array_construct( 'conv.product' ),
            'where_clause','pets <> \'BIRD\'',
            'consumer_db','consumer_db',
            'consumer_schema','consumer_schema',
            'consumer_customer_table','consumer_table',
            'consumer_conversions_table','conversions_table',
            'consumer_email_field','email',
            'consumer_phone_field','phone',
            'consumer_internal_join_field','email',
            'app_instance','consumer_app_instance_db',
            'app_data','dcr_samp_provider_db',
            'at_timestamp',SYSDATE()
            ));
*/
