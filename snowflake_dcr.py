import re
import io
import glob
import os


class SnowflakeDcr:
    """
    A class used to represent a Snowflake data clean room or ID resolution native app
    To use:
        - instantiate the class
        - run a prepare_ method to populate attributes
        - run an execute_ method to generate or run scripts

    Attributes are populated by prepare_ methods
    """

    def __init__(self):
        self.is_debug_mode = None
        self.path = None
        self.script_list = None
        self.script_conn_list = None
        self.check_words = None
        self.replace_words = None
        self.prepared_script_dict = {}
        self.cleaned_script_dict = {}

    def execute(self):
        """
        Runs a series of SQL scripts with some automated replacements
        """
        if self.is_debug_mode is None or self.path is None or self.script_list is None:
            print("Run a prepare script first!")
        else:
            # Prepare scripts
            comment_code_regex_slash = re.compile(r"(?<!:)//.*")
            comment_code_regex_dash = re.compile(r"(?<!:)--.*")
            snowsql_code_regex = re.compile(r"^(?<!:)!.*")

            for current_script in self.script_list:
                print("Starting " + current_script)
                original_script = current_script
                original_script_full_path = self.path + original_script
                original_script_no_path = os.path.basename(original_script_full_path)
                script_index = self.script_list.index(current_script)
                script_conn = self.script_conn_list[script_index]
                prepared_script_text = ""
                cleaned_script_text = ""

                # Prepared scripts still contain comments
                with open(original_script_full_path, "r", encoding='utf-8') as fin:
                    for line in fin:
                        # Replace values
                        for check, replace in zip(self.check_words, self.replace_words):
                            line = line.replace(check, replace)

                        # Remove SnowSQL lines to enable easier running in worksheets
                        line = re.sub(snowsql_code_regex, '', line)

                        prepared_script_text += line

                        # Remove commented SQL lines due to finicky execute_stream behavior
                        line = re.sub(comment_code_regex_slash, '', line)
                        line = re.sub(comment_code_regex_dash, '', line)

                        cleaned_script_text += line

                    self.prepared_script_dict[original_script_no_path] = prepared_script_text
                    self.cleaned_script_dict[original_script_no_path] = cleaned_script_text

                if not self.is_debug_mode and script_conn is not None:
                    print("Running statements for " + current_script)

                    # Run script
                    f = io.StringIO(cleaned_script_text)
                    for cur in script_conn.execute_stream(f, remove_comments=True):
                        print(cur.query)
                        for ret in cur:
                            print(ret)
                else:
                    print("Debug mode: Script generated but not run for " + current_script)

    def prepare_dcr_deployment(self, is_debug_mode, dcr_version, provider_account, provider_conn, consumer_account,
                               consumer_conn, abbreviation, path, data_selection=None, deployment_type=None):
        """
        Prepares object to deploy 2-party DCRs
        """
        # prepare accounts
        provider_account = provider_account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        script_list = []
        script_conn_list = []

        if dcr_version == "DCR 6.0 Native App":
            if abbreviation == "":
                abbreviation = "demo"

            script_list = ["provider_init.sql", "provider_templates.sql",
                           "consumer_init.sql",
                           "provider_enable_consumer.sql", "provider_ml.sql",
                           "consumer_ml.sql",
                           "consumer_request.sql"]
            script_conn_list = [provider_conn, provider_conn,
                                consumer_conn,
                                provider_conn, provider_conn,
                                consumer_conn,
                                None]

            check_words = ["SNOWCAT2", "snowcat2", "SNOWCAT", "snowcat", "_DEMO_", "_demo_"]
            replace_words = [provider_account, provider_account, consumer_account, consumer_account,
                             "_" + abbreviation +
                             "_", "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words
        elif dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"

            # Determine scripts by data selection
            if data_selection == "Media & Advertising":
                use_case_directory = "media-and-advertising"
            elif data_selection == "Supply Chain":
                use_case_directory = "supply-chain"
            elif data_selection == "Financial Services":
                use_case_directory = "financial-services"
            else:
                use_case_directory = None

            if deployment_type == "Provider":
                if use_case_directory is None:
                    script_list = ["provider_init.sql"]
                    script_conn_list = [provider_conn]
                else:
                    script_list = ["provider_init.sql", use_case_directory + "/provider_data.sql",
                                   use_case_directory + "/provider_templates.sql"]
                    script_conn_list = [provider_conn, provider_conn,
                                        provider_conn]
            elif deployment_type == "Consumer":
                if use_case_directory is None:
                    script_list = ["consumer_init.sql",
                                   "provider_enable_consumer.sql"]
                    script_conn_list = [consumer_conn,
                                        provider_conn]
                else:
                    script_list = ["consumer_init.sql", use_case_directory + "/consumer_data.sql",
                                   "provider_enable_consumer.sql",
                                   use_case_directory + "/consumer_request.sql"]
                    script_conn_list = [consumer_conn, consumer_conn,
                                        provider_conn,
                                        None]
            else:
                if use_case_directory is None:
                    script_list = ["provider_init.sql",
                                   "consumer_init.sql",
                                   "provider_enable_consumer.sql"]
                    script_conn_list = [provider_conn,
                                        consumer_conn,
                                        provider_conn]
                else:
                    script_list = ["provider_init.sql", use_case_directory + "/provider_data.sql",
                                   use_case_directory + "/provider_templates.sql",
                                   "consumer_init.sql", use_case_directory + "/consumer_data.sql",
                                   "provider_enable_consumer.sql",
                                   use_case_directory + "/consumer_request.sql"]
                    script_conn_list = [provider_conn, provider_conn,
                                        provider_conn,
                                        consumer_conn, consumer_conn,
                                        provider_conn,
                                        None]

            check_words = ["PROVIDER_ACCT", "provider_acct", "CONSUMER_ACCT", "consumer_acct", "_SAMP_", "_samp_"]
            replace_words = [provider_account, provider_account, consumer_account, consumer_account,
                             "_" + abbreviation +
                             "_", "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_data_onboarding(self, is_debug_mode, dcr_version, provider_account, provider_conn, abbreviation, path,
                                source_database, source_schema, source_table, source_column):
        """
        Prepares deployment of custom data table/view
        """
        # prepare account
        provider_account = provider_account.split(".")[0].upper()

        script_list = []
        script_conn_list = []

        # if dcr_version == "DCR 6.0 Native App": TODO - Add later
        if dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"

            script_list = ["provider_data_onboarding.sql"]
            script_conn_list = [provider_conn]

            check_words = ["PROVIDER_ACCT", "provider_acct", "_SAMP_", "_samp_",
                           "SOURCE_DATABASE", "source_database",
                           "SOURCE_SCHEMA", "source_schema",
                           "SOURCE_TABLE", "source_table",
                           "SOURCE_COLUMN", "source_column"]
            replace_words = [provider_account, provider_account,
                             "_" + abbreviation +
                             "_", "_" + abbreviation + "_",
                             source_database, source_database,
                             source_schema, source_schema,
                             source_table, source_table,
                             source_column, source_column]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_template_deployment(self, is_debug_mode, dcr_version, provider_account, provider_conn, consumer_account,
                                    template_name, template_text, available_dimensions, abbreviation, path):
        """
        Prepares deployment of a custom template
        """
        # prepare accounts
        provider_account = provider_account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        # prepare dimensions
        if available_dimensions is None or available_dimensions == '':
            available_dimensions = "''"

        script_list = []
        script_conn_list = []

        # if dcr_version == "DCR 6.0 Native App": TODO - Add later
        if dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"

            script_list = ["provider_add_template.sql"]
            script_conn_list = [provider_conn]

            check_words = ["PROVIDER_ACCT", "provider_acct", "CONSUMER_ACCT", "consumer_acct", "_SAMP_", "_samp_",
                           "NEW_TEMPLATE_NAME", "new_template_name", "NEW_TEMPLATE_TEXT", "new_template_text",
                           "NEW_DIMENSIONS", "new_dimensions"]
            replace_words = [provider_account, provider_account, consumer_account, consumer_account,
                             "_" + abbreviation +
                             "_", "_" + abbreviation + "_",
                             template_name, template_name, template_text, template_text,
                             available_dimensions, available_dimensions]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_consumer_addition(self, is_debug_mode, dcr_version, provider_account, provider_conn, consumer_account,
                                  consumer_conn, abbreviation, path, data_selection=None):
        """
        Prepares object to add consumers to existing DCRs
        """
        # prepare accounts
        provider_account = provider_account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        if dcr_version == "DCR 6.0 Native App":
            if abbreviation == "":
                abbreviation = "demo"

            script_list = ["provider_init_new_consumer.sql",
                           "consumer_init.sql",
                           "provider_enable_consumer.sql", "provider_ml.sql",
                           "consumer_ml.sql",
                           "consumer_request.sql"]
            script_conn_list = [provider_conn,
                                consumer_conn,
                                provider_conn, provider_conn,
                                consumer_conn,
                                None]

            check_words = ["SNOWCAT4", "snowcat4", "SNOWCAT2", "snowcat2", "SNOWCAT", "snowcat", "_DEMO_", "_demo_"]
            replace_words = [consumer_account, consumer_account, provider_account, provider_account, consumer_account,
                             consumer_account, "_" + abbreviation + "_", "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words
        elif dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"

            # Determine scripts by data selection
            if data_selection == "Media & Advertising":
                use_case_directory = "media-and-advertising"
            elif data_selection == "Supply Chain":
                use_case_directory = "supply-chain"
            elif data_selection == "Financial Services":
                use_case_directory = "financial-services"
            else:
                use_case_directory = None

            if use_case_directory is None:
                script_list = ["provider_add_consumer_to_share.sql",
                               "consumer_init.sql",
                               "provider_enable_consumer.sql"]
                script_conn_list = [provider_conn,
                                    consumer_conn,
                                    provider_conn]
            else:
                script_list = [use_case_directory + "/provider_templates.sql", "provider_add_consumer_to_share.sql",
                               "consumer_init.sql", use_case_directory + "/consumer_data.sql",
                               "provider_enable_consumer.sql",
                               use_case_directory + "/consumer_request.sql"]
                script_conn_list = [provider_conn, provider_conn,
                                    consumer_conn, consumer_conn,
                                    provider_conn,
                                    None]

            check_words = ["PROVIDER_ACCT", "provider_acct", "CONSUMER_ACCT", "consumer_acct", "_SAMP_", "_samp_"]
            replace_words = [provider_account, provider_account, consumer_account, consumer_account,
                             "_" + abbreviation +
                             "_", "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_provider_addition(self, is_debug_mode, dcr_version, provider_account, provider_conn, consumer_account,
                                  consumer_conn, abbreviation, app_suffix, path, data_selection=None):
        """
        Prepares object to add providers to existing DCRs
        """
        # prepare accounts
        provider_account = provider_account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        # if dcr_version == "DCR 6.0 Native App": TODO - Add later
        if dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"
            if app_suffix == "":
                app_suffix = "two"

            # Determine scripts by data selection
            if data_selection == "Media & Advertising":
                use_case_directory = "media-and-advertising"
            elif data_selection == "Supply Chain":
                use_case_directory = "supply-chain"
            elif data_selection == "Financial Services":
                use_case_directory = "financial-services"
            else:
                use_case_directory = None

            if use_case_directory is None:
                script_list = ["provider_init.sql",
                               "consumer_init_new_provider.sql",
                               "provider_enable_consumer.sql"]
                script_conn_list = [provider_conn, provider_conn,
                                    consumer_conn,
                                    provider_conn]
            else:
                script_list = ["provider_init.sql",
                               use_case_directory + "/provider_data.sql",
                               use_case_directory + "/provider_templates.sql",
                               "consumer_init_new_provider.sql",
                               "provider_enable_consumer.sql",
                               use_case_directory + "/consumer_request.sql"]
                script_conn_list = [provider_conn,
                                    provider_conn,
                                    provider_conn,
                                    consumer_conn,
                                    provider_conn,
                                    None]

            check_words = ["dcr_samp_app_two", "DCR_SAMP_APP_TWO",
                           "PROVIDER2_ACCT", "provider2_acct",
                           "PROVIDER_ACCT", "provider_acct",
                           "CONSUMER_ACCT", "consumer_acct",
                           "_SAMP_", "_samp_"]
            replace_words = ["dcr_samp_app_" + app_suffix, "dcr_samp_app_" + app_suffix,
                             provider_account, provider_account,
                             provider_account, provider_account,
                             consumer_account, consumer_account,
                             "_" + abbreviation + "_", "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_upgrade(self, is_debug_mode, provider_account, provider_conn, consumer_account, consumer_conn,
                        new_abbreviation, old_abbreviation, path):
        """
        Prepares object to upgrade DCRs from v5.5 to v6.0
        """
        # prepare accounts
        provider_account = provider_account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        if new_abbreviation == "":
            new_abbreviation = "demo"

        if old_abbreviation == "":
            old_abbreviation = "samp"

        script_list = ["provider_init.sql", "provider_upgrade.sql",
                       "consumer_init.sql",
                       "provider_enable_consumer.sql", "provider_ml.sql",
                       "consumer_ml.sql"
                       "consumer_request.sql"]
        script_conn_list = [provider_conn, provider_conn,
                            consumer_conn,
                            provider_conn, provider_conn,
                            consumer_conn,
                            None]

        check_words = ["SNOWCAT2", "snowcat2", "SNOWCAT", "snowcat", "_DEMO_", "_demo_", "_SAMP_", "_samp_"]
        replace_words = [provider_account, provider_account, consumer_account, consumer_account,
                         "_" + new_abbreviation + "_",
                         "_" + new_abbreviation + "_", "_" + old_abbreviation + "_", "_" + old_abbreviation + "_"]

        self.is_debug_mode = is_debug_mode
        self.path = path
        self.script_list = script_list
        self.script_conn_list = script_conn_list
        self.check_words = check_words
        self.replace_words = replace_words

    def prepare_uninstall(self, is_debug_mode, dcr_version, account_type, account, account_conn, consumer_account,
                          abbreviation, app_suffix, path):
        """
        Prepares object to uninstall DCRs for an account (provider or consumer)
        """
        # prepare accounts
        account = account.split(".")[0].upper()
        consumer_account = consumer_account.split(".")[0].upper()

        if dcr_version == "DCR 6.0 Native App":
            if abbreviation == "":
                abbreviation = "demo"

            check_words = ["SNOWCAT2", "snowcat2", "SNOWCAT", "snowcat", "_DEMO_", "_demo_"]
            replace_words = [account, account, consumer_account, consumer_account, "_" + abbreviation + "_",
                             "_" + abbreviation + "_"]

            if account_type == "Provider":
                script_list = ["provider_uninstall.sql"]
                script_conn_list = [account_conn]
            elif account_type == "Consumer":
                script_list = ["consumer_uninstall.sql"]
                script_conn_list = [account_conn]
                # Support for multi-provider
                if app_suffix != "":
                    check_words.append("_app")
                    replace_words.append("_app_" + app_suffix)
                    check_words.append("_APP")
                    replace_words.append("_app_" + app_suffix)
            else:
                script_list = []
                script_conn_list = []

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words
        elif dcr_version == "DCR 5.5 General Availability":
            if abbreviation == "":
                abbreviation = "samp"

            if account_type == "Provider":
                script_list = ["provider_uninstall.sql"]
                script_conn_list = [account_conn]
            elif account_type == "Consumer":
                script_list = ["consumer_uninstall.sql"]
                script_conn_list = [account_conn]
            else:
                script_list = []
                script_conn_list = []

            check_words = ["PROVIDER_ACCT", "provider_acct", "CONSUMER_ACCT", "consumer_acct", "_SAMP_", "_samp_"]
            replace_words = [account, account, consumer_account, consumer_account, "_" + abbreviation + "_",
                             "_" + abbreviation + "_"]

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words

    def prepare_id_res_deployment(self, is_debug_mode, id_res_version, account, account_conn, account_type,
                                  options, path):
        """
        Prepares object to deploy ID Resolution Native App
        """
        # prepare accounts
        account = account.split(".")[0].upper()

        if id_res_version == "ID Resolution Native App":
            script_list = []
            script_conn_list = []

            if account_type == "Provider":
                path = path + "provider/app/"
            else:
                path = path + "consumer/app/"

            for file in glob.iglob(path + "*"):
                script_list.append(os.path.basename(file))
                script_conn_list.append(account_conn)

            # List of connections does not need sorted, since they are all the same value
            script_list.sort()

            check_words = []
            replace_words = []

            for key, value in options.items():
                check_words.append("&" + key)
                replace_words.append(value.upper())
                check_words.append("&" + key.upper())
                replace_words.append(value.upper())

            self.is_debug_mode = is_debug_mode
            self.path = path
            self.script_list = script_list
            self.script_conn_list = script_conn_list
            self.check_words = check_words
            self.replace_words = replace_words
