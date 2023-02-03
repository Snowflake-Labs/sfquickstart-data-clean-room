/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Consumer Initialization
Create Date:        2022-02-09
Author:             J. Langseth, M. Rainey
Description:        Cleanroom Consumer side object and data initialization


Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-02-09          J. Langseth, M. Rainey              Initial Creation
2022-04-01          V. Malik                            v5.5 [Version without Native app , with JINJASQL
                                                        template & DP]
2022-04-13          M. Rainey                           Added SQL based templates & get_sql_js function. Form
                                                        query in consumer side.
2022-06-30          M. Rainey                           Added provider-specific schema for request proc and
                                                        requests table in support of multi-party.
                                                        Modified use of get_sql_js function to use shared
                                                        function from Provider.
2022-07-06          B. Klein                            Added new user_settings for multi-party and comments.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_ to support upgrades.
2022-07-12          B. Klein                            Updated get_sql_jinja to allow negative values.
2022-08-05          B. Klein                            Uncommented get_sql_jinja to facilitate DCR Assistant
2022-08-23          M. Rainey                           Remove differential privacy
2022-08-30          D. Cole, B. Klein                   Added new javascript jinja template engine
2022-10-25          B. Klein                            Separated framework code and demo data
2022-11-08          B. Klein                            Python GA
2023-02-02          B. Klein                            Added object comments for clarity
*************************************************************************************************************/

use role accountadmin;

create role if not exists data_clean_room_role;

// may need to drop and re-add warehouse if already created by accountadmin
//drop warehouse app_wh;

grant create share on account to role data_clean_room_role;
grant import share on account to role data_clean_room_role;
grant create database on account to role data_clean_room_role;
grant create warehouse on account to role data_clean_room_role;
grant role data_clean_room_role to role sysadmin;
use role data_clean_room_role;

create warehouse if not exists app_wh;

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

///////
// CONSUMER_ACCT LOCAL SETUP
///////

create or replace database dcr_samp_consumer;
create or replace schema dcr_samp_consumer.util;

// Python jinja function
create or replace function dcr_samp_consumer.util.get_sql_jinja(template string, parameters variant)
  returns string
  language python
  runtime_version = 3.8
  handler='apply_sql_template'
  packages = ('six','jinja2==3.0.3','markupsafe')
  comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}'
as
$$
# Most of the following code is copied from the jinjasql package, which is not included in Snowflake's python packages
from __future__ import unicode_literals
import jinja2
from six import string_types
from copy import deepcopy
import os
import re
from jinja2 import Environment
from jinja2 import Template
from jinja2.ext import Extension
from jinja2.lexer import Token
from markupsafe import Markup

try:
    from collections import OrderedDict
except ImportError:
    # For Python 2.6 and less
    from ordereddict import OrderedDict

from threading import local
from random import Random

_thread_local = local()

# This is mocked in unit tests for deterministic behaviour
random = Random()


class JinjaSqlException(Exception):
    pass

class MissingInClauseException(JinjaSqlException):
    pass

class InvalidBindParameterException(JinjaSqlException):
    pass

class SqlExtension(Extension):

    def extract_param_name(self, tokens):
        name = ""
        for token in tokens:
            if token.test("variable_begin"):
                continue
            elif token.test("name"):
                name += token.value
            elif token.test("dot"):
                name += token.value
            else:
                break
        if not name:
            name = "bind#0"
        return name

    def filter_stream(self, stream):
        """
        We convert
        {{ some.variable | filter1 | filter 2}}
            to
        {{ ( some.variable | filter1 | filter 2 ) | bind}}
        ... for all variable declarations in the template
        Note the extra ( and ). We want the | bind to apply to the entire value, not just the last value.
        The parentheses are mostly redundant, except in expressions like {{ '%' ~ myval ~ '%' }}
        This function is called by jinja2 immediately
        after the lexing stage, but before the parser is called.
        """
        while not stream.eos:
            token = next(stream)
            if token.test("variable_begin"):
                var_expr = []
                while not token.test("variable_end"):
                    var_expr.append(token)
                    token = next(stream)
                variable_end = token

                last_token = var_expr[-1]
                lineno = last_token.lineno
                # don't bind twice
                if (not last_token.test("name")
                    or not last_token.value in ('bind', 'inclause', 'sqlsafe')):
                    param_name = self.extract_param_name(var_expr)

                    var_expr.insert(1, Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'rparen', u')'))
                    var_expr.append(Token(lineno, 'pipe', u'|'))
                    var_expr.append(Token(lineno, 'name', u'bind'))
                    var_expr.append(Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'string', param_name))
                    var_expr.append(Token(lineno, 'rparen', u')'))

                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            else:
                yield token

def sql_safe(value):
    """Filter to mark the value of an expression as safe for inserting
    in a SQL statement"""
    return Markup(value)

def bind(value, name):
    """A filter that prints %s, and stores the value
    in an array, so that it can be bound using a prepared statement
    This filter is automatically applied to every {{variable}}
    during the lexing stage, so developers can't forget to bind
    """
    if isinstance(value, Markup):
        return value
    elif requires_in_clause(value):
        raise MissingInClauseException("""Got a list or tuple.
            Did you forget to apply '|inclause' to your query?""")
    else:
        return _bind_param(_thread_local.bind_params, name, value)

def bind_in_clause(value):
    values = list(value)
    results = []
    for v in values:
        results.append(_bind_param(_thread_local.bind_params, "inclause", v))

    clause = ",".join(results)
    clause = "(" + clause + ")"
    return clause

def _bind_param(already_bound, key, value):
    _thread_local.param_index += 1
    new_key = "%s_%s" % (key, _thread_local.param_index)
    already_bound[new_key] = value

    param_style = _thread_local.param_style
    if param_style == 'qmark':
        return "?"
    elif param_style == 'format':
        return "%s"
    elif param_style == 'numeric':
        return ":%s" % _thread_local.param_index
    elif param_style == 'named':
        return ":%s" % new_key
    elif param_style == 'pyformat':
        return "%%(%s)s" % new_key
    elif param_style == 'asyncpg':
        return "$%s" % _thread_local.param_index
    else:
        raise AssertionError("Invalid param_style - %s" % param_style)

def requires_in_clause(obj):
    return isinstance(obj, (list, tuple))

def is_dictionary(obj):
    return isinstance(obj, dict)

class JinjaSql(object):
    # See PEP-249 for definition
    # qmark "where name = ?"
    # numeric "where name = :1"
    # named "where name = :name"
    # format "where name = %s"
    # pyformat "where name = %(name)s"
    VALID_PARAM_STYLES = ('qmark', 'numeric', 'named', 'format', 'pyformat', 'asyncpg')
    def __init__(self, env=None, param_style='format'):
        self.env = env or Environment()
        self._prepare_environment()
        self.param_style = param_style

    def _prepare_environment(self):
        self.env.autoescape=True
        self.env.add_extension(SqlExtension)
        self.env.filters["bind"] = bind
        self.env.filters["sqlsafe"] = sql_safe
        self.env.filters["inclause"] = bind_in_clause

    def prepare_query(self, source, data):
        if isinstance(source, Template):
            template = source
        else:
            template = self.env.from_string(source)

        return self._prepare_query(template, data)

    def _prepare_query(self, template, data):
        try:
            _thread_local.bind_params = OrderedDict()
            _thread_local.param_style = self.param_style
            _thread_local.param_index = 0
            query = template.render(data)
            bind_params = _thread_local.bind_params
            if self.param_style in ('named', 'pyformat'):
                bind_params = dict(bind_params)
            elif self.param_style in ('qmark', 'numeric', 'format', 'asyncpg'):
                bind_params = list(bind_params.values())
            return query, bind_params
        finally:
            del _thread_local.bind_params
            del _thread_local.param_style
            del _thread_local.param_index

# Non-JinjaSql package code starts here
def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        #baseline sql injection deterrance
        new_value2 = re.sub(r"[^a-zA-Z0-9_.-]","",new_value)
        return "'{}'".format(new_value2)
    return value

def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params

def strip_blank_lines(text):
    '''
    Removes blank lines from the text, including those containing only spaces.
    https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
    '''
    return os.linesep.join([s for s in text.splitlines() if s.strip()])

def apply_sql_template(template, parameters):
    '''
    Apply a JinjaSql template (string) substituting parameters (dict) and return
    the final SQL.
    '''
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(template, parameters)
    return strip_blank_lines(get_sql_from_template(query, bind_params))

$$;


//Additonal steps for V5.5
create or replace schema dcr_samp_consumer.PROVIDER_ACCT_schema;
create table dcr_samp_consumer.util.instance 
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}' 
as select randstr(64,random()) as id;


///////
// CONSUMER_ACCT INSTALLS THE CLEAN ROOM APP - PROVIDER_ACCT_1
///////

// mount clean room app
create or replace database dcr_samp_app from share PROVIDER_ACCT.dcr_samp_app;

// see templates from provider
select * from dcr_samp_app.cleanroom.templates;


///////
// CONSUMER_ACCT SETS UP REQUEST TABLES AND SHARES
///////

// create the request tables
create or replace table dcr_samp_consumer.PROVIDER_ACCT_schema.requests (request_id varchar(1000),request variant, signature varchar(1000))
comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}';
ALTER TABLE dcr_samp_consumer.PROVIDER_ACCT_schema.requests SET CHANGE_TRACKING = TRUE;

// share the request table to the provider
create or replace share dcr_samp_requests_PROVIDER_ACCT;
grant usage on database dcr_samp_consumer to share dcr_samp_requests_PROVIDER_ACCT;
grant usage on schema dcr_samp_consumer.PROVIDER_ACCT_schema to share dcr_samp_requests_PROVIDER_ACCT;
grant select on dcr_samp_consumer.PROVIDER_ACCT_schema.requests to share dcr_samp_requests_PROVIDER_ACCT;
alter share dcr_samp_requests_PROVIDER_ACCT add accounts = PROVIDER_ACCT;

// test share from app, see app instance unique id - provider1
//select * from dcr_samp_app.cleanroom.instance;


///////
// creating Request stored proc
///////

create or replace procedure dcr_samp_consumer.PROVIDER_ACCT_schema.request(in_template varchar(1000), in_params varchar(10000), request_id varchar(1000), at_timestamp VARCHAR(30))
    returns variant
    language javascript
    comment='{"origin":"sf_ps_wls","name":"dcr","version":{"major":5, "minor":5},"attributes":{"component":"dcr",“role”:“consumer”}}'
    execute as owner as
    $$
    // get the provider account
    var result = snowflake.execute({ sqlText:  `select account_name from dcr_samp_app.cleanroom.provider_account ` });
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
            (select template from dcr_samp_app.cleanroom.templates where template_name = rp.query_template), qpf.request_params) as proposed_query,
            sha2(proposed_query) as proposed_query_hash from query_params_full qpf, request_params rp)
     select rp.*, pq.*, f.request_params
        from query_params_full f, request_params rp, proposed_query pq )
     select object_construct(*) as request from all_params;`;

  var result = snowflake.execute({ sqlText: request_sql });
  result.next();
  var request = result.getColumnValue(1);

  // put request JSON into a temporary place so if it is approved we can use it later directly from SQL to avoid JS altering it
   var result = snowflake.execute({ sqlText: `create or replace table dcr_samp_consumer.PROVIDER_ACCT_schema.request_temp as select REQUEST FROM      table(result_scan(last_query_id()));` });

  var signed_request ='';

 //insert the signed request
  var insert_sql =  `insert into dcr_samp_consumer.PROVIDER_ACCT_schema.requests (request_id , request, signature )
  select (select any_value(request:REQUEST_ID) from PROVIDER_ACCT_schema.request_temp),(select any_value(request) from PROVIDER_ACCT_schema.request_temp), '`+signed_request+`' ; `;
  var r_stmt = snowflake.createStatement( { sqlText: insert_sql } );
  var result = r_stmt.execute();

  var result = snowflake.execute({ sqlText:`select any_value(request:REQUEST_ID) from PROVIDER_ACCT_schema.request_temp` });
  result.next();
  var request_id = result.getColumnValue(1);

  return [ "Request Sent", request_id , request];

$$;
