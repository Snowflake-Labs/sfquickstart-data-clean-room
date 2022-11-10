# data-clean-room-quickstart
Copyright (c) 2022 Snowflake Inc. All Rights Reserved.

This sample code is provided for reference purposes only.  Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.


This version of data clean room (v5.5) has these major enhancements from v5:

1. Uses JinjaSQL templates to express allowed clean room query patterns between 2+ parties
2. Consumers make requests via a simpler "call cleanroom.request(template, parameters)" procedure
3. Includes example query templates for common advertising scenarios, including 3+ party clean room queries
4. Designed so that any provider can use it with any consumer
5. Request process + templates allows for easy upgrade process to v6 (native apps based)

# Deploying this code

#### Option 1: Automated script generation via hosted [Streamlit](https://streamlit.io/)
- Go to our Streamlit DCR Assistant Streamlit app [here](https://snowflake-labs-sfquickstart-data-cle-dcr-setup-assistant-bkx7gg.streamlitapp.com/)
- Enter a database abbreviation, provider & consumer account locators, run, and download
- Run the scripts in the order below

#### Option 2: Manual find/replace to prepare scripts
Update all references to account CONSUMER_ACCT to the consumer account name and account PROVIDER_ACCT to the provider account name in the sql scripts in the data-clean-room directory.

This code enables computation across two or more parties. Prior to creating a Python UDF, you must acknowledge the Snowflake Third Party Terms following steps here:
https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started

Templates utilize [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) and [JinjaSQL](https://github.com/sripathikrishnan/jinjasql) to enable flexible, yet controllable question definitions.

#### Finally - Running the scripts
Each script can be run as a batch, except consumer_request, which should be executed line-by-line.  Runs scripts that start with "provider_" on the provider account, and likewise for consumer. Run in this order:

1. provider_init
2. provider_data
3. provider_templates
4. consumer_init
5. consumer_data
4. provider_enable_consumer
5. consumer_request

# Add a new consumer

Update all references to account CONSUMER_ACCT to the new consumer account name and account PROVIDER_ACCT to the provider account name.

Run the following scripts to add a new Consumer account to an existing data clean room. This assumes that the code has already been deployed across two Snowflake accounts.

1. provider_templates
2. provider_add_consumer_to_share
3. consumer_init
4. consumer_data
5. provider_enable_new_consumer
6. consumer_request

# Add a new provider for multi-party

Update all references to account CONSUMER_ACCT to the consumer account name.

Run the following scripts to add a new Provider account to an existing data clean room Consumer. This assumes that the code has already been deployed across two Snowflake accounts.

1. Provider 2 runs provider_init - must change reference to PROVIDER_ACCT to Provider 2 account name
2. Provider runs provider_data - must change reference to PROVIDER_ACCT to Provider 2 account name
3. Provider 2 runs provider_templates
4. Consumer runs consumer_init_new_provider - must change reference to PROVIDER2_ACCT to Provider 2 account name
5. Provider 2 runs provider_enable_consumer - must change PROVIDER_ACCT to Provider 2 account name
6. Consumer tests with request against multiple parties using consumer_request - NOTE: The timestamp in the request must be in UTC for timezone compatibility

# Uninstall Demo

To uninstall the DCR on the provider, update CONSUMER_ACCT to the consumer account name and run provider_uninstall on the provider account.

To uninstall the DCR on the consumer, run consumer_uninstall on the consumer account.
