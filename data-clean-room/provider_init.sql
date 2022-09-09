/*************************************************************************************************************
Script:             Data Clean Room - v5.5 - Provider Initialization
Create Date:        2022-02-09
Author:             J. Langseth, M. Rainey
Description:        Provider object and data initialization

Copyright © 2022 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2022-02-09          J. Langseth, M. Rainey              Initial Creation
2022-04-01          V. Malik                            v5.5 [Version without Native app , with JINJASQL
                                                        template & DP]
2022-04-13          M. Rainey                           Added SQL based templates & get_sql_js function.
2022-06-30          M. Rainey                           Updated get_sql_js params in support of multi-party.
                                                        Modified schema location for get_sql_js function so
                                                        funcction can be shared with Consumer.
2022-07-06          B. Klein                            Added new subscriptions data and included in share to
                                                        demo with multi-party.
2022-07-11          B. Klein                            Renamed _demo_ to _samp_  and added template_type to
                                                        templates to better support upgrades.
2022-07-12          B. Klein                            Updated get_sql_jinja to allow negative values.
2022-08-05          B. Klein                            Uncommented get_sql_jinja to facilitate DCR Assistant
2022-08-23          M. Rainey                           Remove differential privacy
2022-08-30          D. Cole, B. Klein                   Added new javascript jinja template engine
*************************************************************************************************************/


use role accountadmin;
create warehouse if not exists app_wh;

//cleanup//
// drop share if exists dcr_samp_data;
drop share if exists dcr_samp_app;

///////
/// CREATE PROVIDER_ACCT DATA
///////

// create database and schema for the app
create or replace database dcr_samp_provider_db;
//create or replace schema dcr_samp_provider_db.installer_schema;

// create schema for provider objects that app instances can securely utilize
create or replace schema dcr_samp_provider_db.shared_schema;

// generate sample customers with emails and features
create or replace table dcr_samp_provider_db.shared_schema.customers as
select 'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email,
 replace(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||uniform(1, 10, random()),' ','') as phone,
  case when uniform(1,10,random())>3 then 'MEMBER'
       when uniform(1,10,random())>5 then 'SILVER'
       when uniform(1,10,random())>7 then 'GOLD'
else 'PLATINUM' end as status,
round(18+uniform(0,10,random())+uniform(0,50,random()),-1)+5*uniform(0,1,random()) as age_band
  from table(generator(rowcount => 1000000));

create or replace table dcr_samp_provider_db.shared_schema.exposures as
select email, 'campaign_'||uniform(1,3,random()) as campaign,
  case when uniform(1,10,random())>3 then 'STREAMING'
       when uniform(1,10,random())>5 then 'MOBILE'
       when uniform(1,10,random())>7 then 'LINEAR'
else 'DISPLAY' end as device_type,
('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as exp_date,
uniform(1,60,random()) as sec_view,
uniform(0,2,random())+uniform(0,99,random())/100 as exp_cost
from dcr_samp_provider_db.shared_schema.customers sample (20);
select ('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as exp_date;

// generate subscription information for users
create or replace table dcr_samp_provider_db.shared_schema.subscriptions as
select
        'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email
    ,   uniform(0,1,random()) as is_subscribed
from table(generator(rowcount => 1000000));

//select * from dcr_samp_provider_db.shared_schema.customers;
//select * from dcr_samp_provider_db.shared_schema.exposures;
//select * from dcr_samp_provider_db.shared_schema.subscriptions;

/////
// SETUP TEMPLATES
/////

use database dcr_samp_provider_db;
create or replace schema templates;
create or replace schema util;
create or replace schema admin;
create schema dcr_samp_provider_db.cleanroom;

use schema dcr_samp_provider_db.templates;

// Javascript jinja-like interpreter - supports Jinja parsing, but does not actually use the Jinja engine
create or replace secure function dcr_samp_provider_db.cleanroom.get_sql_jinja_js(template string, parameters variant)
    returns string
    language javascript
as
$$
    const template = TEMPLATE;
    const parameters = PARAMETERS;

    // seed local variables (LIFO)
    let localVars = Object.assign({}, ...Object.keys(parameters).map((k) => ({ [k.toLowerCase()]: [ parameters[k] ] })));

    // classify some potential errors
    const NotFound = Symbol('NotFound');
    const NotDefined = Symbol('NotDefined');
    const Unsupported = Symbol('Unsupported');
    const NoValue = Symbol('NoValue');
    const ParseError = Symbol('ParseError');
    class MyError extends Error {
        constructor(type, message) {
            super(message);
            this.type = type
        }
    }

    // variable literals -> variable values
    // (base) variable must exist
    // foo -> $foo
    // foo[i] -> $foo[i]
    // foo[i:] -> $foo.slice(i)
    // foo[:j] -> $foo.slice(0,j)
    // foo[i:j] -> $foo.slice(i,j)
    // foo.split() -> $foo.split() (also ...split('x') and ...split(/x/) and ...split('x', 3) and ...split(/x/, 3))
    // or any combination thereof...
    // more rewrites possible...
    const strToVal = (str) => {
        const getKey = (str) => {
            let key = /^(\w+)/.exec(str)[1];
            if (key == null)
                throw new MyError(NotFound, "variable name not found in '" + str + "'");
            return key;
        };

        const getVal = (key) => {
            let k = key.toLowerCase();
            let val = k in localVars && localVars[k].length ? localVars[k][0] : null;
            if (val == null)
                throw new MyError(NotDefined, "variable '" + key + "' not defined");
            return val;
        };

        try {
            let key = getKey(str);
            let val = getVal(key);
            str = str.substring(key.length);
            if (str.length) {
                let m;
                while (str.length) {
                    switch(true) {
                        case (m = /^(\[(\d+)\])/.exec(str)) != null:
                            val = val[m[2]];
                            break;
                        case (m = /^(\[(\d+):(\d+)?\])/.exec(str)) != null:
                            val = m[3] == null ? val.slice(m[2]) : val.slice(m[2], m[3]);
                            break;
                        case (m = /^(\[(\d+)?:(\d+)\])/.exec(str)) != null:
                            val = m[2] == null ? val.slice(0, m[3]) : val.slice(m[2], m[3]);
                            break;
                        case (m = /^(\.split\((?:\s*(['"/])?(.*?)\2\s*)(?:\s*,\s*(\d*)\s*)?\))/i.exec(str)) != null:
                            let s = m[2] == '/' ? new RegExp(m[3], 'g') : m[3];
                            val = m[3] == null ? val.split() : m[4] == null ? val.split(s) : val.split(s, m[4]);
                            break;
                        default:
                            throw new MyError(Unsupported, "unsupported operator against variable '" + key + "' in '" + str + "'");
                    }
                    str = str.substring(m[1].length);
                }
            }
            if (typeof val === 'undefined')
                throw new MyError(NoValue, "variable '" + key + "' has no value");
            return val;
        }
        catch (error) {
            throw error;
        }
    };

    // white space
    // <value>
    const WhiteSpaceRegex = /^((?:(?!\{[%{#])\s)+)/s;
    class WhiteSpace {
        constructor(args) {
            this.original = args[0];
            this.value = args[1];
        }
        rewrite() {
            return this.value;
        }
    }

    // string literal
    // <value>
    const StringLiteralRegex = /^((?:(?!\{[%{#])\S)+)/s;
    class StringLiteral {
        constructor(args) {
            this.original = args[0];
            this.value = args[1];
        }
        rewrite() {
            return this.value;
        }
    }

    // jinja for statement
    // {% for <iterator> in <iterable> %}
    // <block>
    // {% endfor %}
    // <iterator> and <iterable> cannot be null
    // <iterator> is scoped for this <block>
    // (base) <iterable> must exist
    // <block> is on its own line(s)
    const JinjaForStatementRegex = /^\{%\s*for\s+(.+?)\s+in\s+(.+?)\s*%}(.+?){%\s*endfor\s*%}(?:\n[ \t]+)?/is;
    class JinjaForStatement {
        constructor(args) {
            this.original = args[0];
            this.iterator = args[1];
            this.iterable = args[2];
            this.block = parse(args[3]);
        }
        rewrite() {
            try {
                let result = '';
                let k = this.iterator.toLowerCase();
                if (!(k in localVars))
                    localVars[k] = [];
                let iterable = strToVal(this.iterable);
                for (let iterator in iterable) {
                    localVars[k].unshift(iterable[iterator]);
                    this.block.forEach((b) => { result += b.rewrite(); });
                    localVars[k].shift();
                }
                return result;
            }
            catch (error) {
                throw error;
            }
        }
    }

    // jinja if statement class
    // {% if <variable> %}
    // <block>
    // {% endif %}
    // <variable> cannot be null
    // <block> is rewritten if <variable> exists
    // <block> is on its own line(s)
    const JinjaIfStatementRegex = /^\{%\s*if\s+(.+?)\s*%}(.+?)\{%\s*endif\s*%}(?:\n[ \t]+)?/is;
    class JinjaIfStatement {
        constructor(args) {
            this.original = args[0];
            this.variable = args[1];
            this.block = parse(args[2]);
        }
        rewrite() {
            try {
                let result = '';
                if (strToVal(this.variable))
                    this.block.forEach((b) => { result += b.rewrite(); });
                return result;
            }
            catch (error) {
                if (error.type == NotFound || error.type == NotDefined || error.type == NoValue)
                    return '';
                else
                    throw error;
            }
        }
    }

    // jinja set statement class
    // {% set <variable> = <expr> [{% endset ]%}
    // <variable> and <expr> cannot be null
    // <expr> limited to defined variables and supported functions
    const JinjaSetStatementRegex = /^\{%\s*set\s+(.+?)\s*=\s*(.+?)\s*(?:{%\s*endset\s*)?%}/is;
    class JinjaSetStatement {
        constructor(args) {
            this.original = args[0];
            this.variable = args[1];
            this.expr = args[2];
        }
        rewrite() {
            try {
                let k = this.variable.toLowerCase();
                if (!(k in localVars))
                    localVars[k] = []
                localVars[k].unshift(strToVal(this.expr));
                return '';
            }
            catch (error) {
                throw error;
            }
        }
    }

    // a jinja expression class
    // {{ <expr>[ | <filter> ] }}
    // <expr> must exist
    // <expr> limited to substitutions plus supported operations (slicing, splitting, etc)
    // ignoring <filter> (for now...)
    const JinjaExpressionRegex = /^\{\{\s*(.+?)(?:\s*\|\s*(.+?))?\s*}}/is;
    class JinjaExpression {
        constructor(args) {
            this.original = args[0];
            this.expr = args[1];
            this.filter = args[2];
        }
        rewrite() {
            try {
                let v = strToVal(this.expr);

                if (this.filter == undefined) {
                    return "'" + v + "'";
                }

                switch(this.filter.toLowerCase()) {
                    case 'sqlsafe':
                        return v;
                    case 'inclause':
                        let str = "("
                        v.forEach((element,index) => {
                            if(index==0) {
                                if (typeof element ==='string') {
                                    str += "'" + element + "'";
                                } else {
                                    str += element;
                                }
                            } else {
                                if (typeof element ==='string') {
                                    str += ",'" + element + "'";
                                } else {
                                    str += "," + element;
                                }
                            }
                        })
                        str += ")";
                        return str;
                    default:
                        return "'" + v + "'";
                }
            }
            catch (error) {
                throw error;
            }
        }
    }

    // jinja comment class
    // {# <block> #}
    // comments are stripped from the result
    const JinjaCommentRegex = /^\{#\s*(.*?)\s*#}/is;
    class JinjaComment {
        constructor(args) {
            this.original = args[0];
            this.block = args[1];
        }
        rewrite() {
            return '';
        }
    }

    const parse = str => {
        let result = [];

        let m;
        while (str.length) {
            if ((m = JinjaForStatementRegex.exec(str)) != null)
                result.push(new JinjaForStatement(m));
            else if ((m = JinjaIfStatementRegex.exec(str)) != null)
                result.push(new JinjaIfStatement(m));
            else if ((m = JinjaSetStatementRegex.exec(str)) != null)
                result.push(new JinjaSetStatement(m));
            else if ((m = JinjaExpressionRegex.exec(str)) != null)
                result.push(new JinjaExpression(m));
            else if ((m = JinjaCommentRegex.exec(str)) != null)
                result.push(new JinjaComment(m));
            else if ((m = StringLiteralRegex.exec(str)) != null)
                result.push(new StringLiteral(m));
            else if ((m = WhiteSpaceRegex.exec(str)) != null)
                result.push(new WhiteSpace(m));
            else
                throw new MyError(ParseError, "unable to parse '" + str + "'");
            str = str.substring(m[0].length);
        }

        return result;
    };

    // rewrite the parsed objects
    const rewrite = objects => {
        return objects.map(o => o.rewrite()).join("").split("\n").filter(r => r.trim().length).join("\n");
    };

    return rewrite(parse(template));
$$;

// Python jinja function - actually uses the Jinja engine, but requries Python UDFs
// The function is not used unless swapped in the requests() and process_requests() procedures
/*python
create or replace function dcr_samp_provider_db.templates.get_sql_jinja_py(template string, parameters variant)
  returns string
  language python
  runtime_version = 3.8
  handler='apply_sql_template'
  packages = ('six','jinja2')
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
from jinja2.utils import Markup

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
        self.env.add_extension('jinja2.ext.autoescape')
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
python*/


//////
// CREATE PROVIDER_ACCT ACCOUNT TABLE
//////

create or replace table dcr_samp_provider_db.cleanroom.provider_account(account_name varchar(1000));

// do this for each consumer account
insert into dcr_samp_provider_db.cleanroom.provider_account (account_name)
select current_account();


//////
// CLEAN ROOM TEMPLATES
//////

// TODO CONFIRM THIS IS ALL SQL SAFE (partially done with identifier and string parsing in jinja renderer)

create or replace table dcr_samp_provider_db.templates.dcr_templates (party_account varchar(1000) ,template_name string, template string, dp_sensitivity int, dimensions varchar(2000), template_type string);

// create a view to allow each consumer account to see their templates
create or replace secure view dcr_samp_provider_db.cleanroom.templates as
select * from dcr_samp_provider_db.templates.dcr_templates  where current_account() = party_account;

// if you want to edit the templates, run this then re-insert them
// delete from dcr_samp_provider_db.templates.dcr_templates;


//////////
// CREATE CLEAN ROOM UTILITY FUNCTIONS
//////////

// add RAP to dcr_samp_provider_db.shared_schema.customers

// see that customer table is not yet protected with a data firewall
select * from dcr_samp_provider_db.shared_schema.customers;

// create a request tracking table that will also contain allowed statements
create or replace table dcr_samp_provider_db.admin.request_log
    (party_account varchar(1000), request_id varchar(1000), request_ts timestamp, request variant, query_hash varchar(1000),
     template_name varchar(1000), epsilon double, sensitivity int,  app_instance_id varchar(1000), processed_ts timestamp, approved boolean, error varchar(1000));

// create a dynamic secure view to allow each consumer to only see the status of their request rows
create or replace secure view dcr_samp_provider_db.cleanroom.provider_log as
select * from dcr_samp_provider_db.admin.request_log where current_account() = party_account;

//////////////////
// Protect providers base table with Data Firewall
//////////////////

//alter session set ENABLE_ROW_ACCESS_POLICY = true;

// shields down
//alter table dcr_samp_provider_db.shared_schema.customers drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;
//alter table dcr_samp_provider_db.shared_schema.exposures drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;
//alter table dcr_samp_provider_db.shared_schema.subscriptions drop row access policy dcr_samp_provider_db.shared_schema.data_firewall;

create or replace row access policy dcr_samp_provider_db.shared_schema.data_firewall as (foo varchar) returns boolean ->
    exists  (select request_id from dcr_samp_provider_db.admin.request_log w
               where party_account=current_account()
                  and approved=true
                  and query_hash=sha2(current_statement()));

//see request log
select * from dcr_samp_provider_db.admin.request_log;

// shields up
alter table dcr_samp_provider_db.shared_schema.customers add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);
alter table dcr_samp_provider_db.shared_schema.exposures add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);
alter table dcr_samp_provider_db.shared_schema.subscriptions add row access policy dcr_samp_provider_db.shared_schema.data_firewall on (email);

// test RAP
select * from dcr_samp_provider_db.shared_schema.customers;  // should now return no rows
select * from dcr_samp_provider_db.shared_schema.exposures;  // should now return no rows
select * from dcr_samp_provider_db.shared_schema.subscriptions;  // should now return no rows

// create the view and schema to share the protected provider data
create or replace secure view dcr_samp_provider_db.cleanroom.provider_data as select * from  dcr_samp_provider_db.shared_schema.customers;
create or replace secure view dcr_samp_provider_db.cleanroom.provider_exposure_data as select * from dcr_samp_provider_db.shared_schema.exposures;
create or replace secure view dcr_samp_provider_db.cleanroom.provider_subscription_data as select * from dcr_samp_provider_db.shared_schema.subscriptions;

//////
// share cleanroom
//////

// Share 1: Out of Band share from P-C for Ps data
// Note: this share was meant to mimic a v6 workaround that no longer exists and will be combined with dcr_samp_app share

//create or replace share dcr_samp_data;
//grant usage on database dcr_samp_provider_db to share dcr_samp_data;
//grant usage on schema dcr_samp_provider_db.cleanroom to share dcr_samp_data;

//alter share dcr_samp_data add accounts = CONSUMER_ACCT;

// SHARE 2: the clean room Application Share
// create application share : Updated using normal share without native app
create or replace share dcr_samp_app ;

// make required grants
grant usage on database dcr_samp_provider_db to share dcr_samp_app;
grant usage on schema dcr_samp_provider_db.cleanroom to share dcr_samp_app;
//grant select on dcr_samp_provider_db.cleanroom.keys_share to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_log to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_account to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.templates to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_data to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_exposure_data to share dcr_samp_app;
grant select on dcr_samp_provider_db.cleanroom.provider_subscription_data to share dcr_samp_app;
GRANT USAGE ON FUNCTION dcr_samp_provider_db.cleanroom.get_sql_jinja_js(string, variant) TO SHARE dcr_samp_app;
alter share dcr_samp_app add accounts = CONSUMER_ACCT;

/// ---> NOW, COMPLETE CONSUMER_ACCT SIDE SETUP
