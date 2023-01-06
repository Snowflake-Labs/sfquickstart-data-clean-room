import io
import streamlit as st
import snowflake_dcr as dcr
import os
from zipfile import ZipFile

# Page settings
st.set_page_config(
    page_title="DCR Setup Assistant",
    page_icon="❄️️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "This app generates scripts for data clean rooms!"
    }
)

# Set up main page
col1, col2 = st.columns((6, 1))
col1.title("🐻‍❄ DCR SETUP ASSISTANT 🐻‍❄️")
col2.image("assets/snowflake_dcr_multi.png", width=120)
st.sidebar.image("assets/bear_snowflake_hello.png")
action = st.sidebar.radio("What action would you like to take?", ("Initial Deployment 🐻‍❄",
                                                                  # "Add Add'l Consumer 🐧️",
                                                                  # "Add Add'l Provider ☃️",
                                                                  "Uninstall 💧"))

st.markdown("Generates scripts for managing version 5.5 Data Clean Rooms.")
st.markdown("This sample code is provided for reference purposes only. Please note that this code is provided “AS IS” "
            "and without warranty.")
st.markdown("Snowflake will not offer any support for use of the sample code.")

dcr_data_options = ['Media & Advertising', 'None']

# Create dcr object
data_clean_room = dcr.SnowflakeDcr()
zip_buffer = io.BytesIO()


# Gets version info
def get_version_info(repo_path):
    version_file = repo_path + "VERSION.md"
    version_list = []
    # Read version file
    try:
        with open(version_file, "r", encoding='utf-8') as fin:
            for line in fin:
                version_list.append(line)
    finally:
        return version_list


# Used to load the zip file buffer in-memory
def load_zip_buffer(snowflake_dcr, buffer, do_include_comments):
    # Add script number to help sort and show the correct order
    script_number = 1
    if do_include_comments:
        with ZipFile(buffer, "w") as archive:
            for script in snowflake_dcr.prepared_script_dict.keys():
                # Remove path
                script_name = script.lstrip("/")

                # Add number
                script_name = str(script_number) + " - " + script_name
                script_number += 1

                archive.writestr(script_name, snowflake_dcr.prepared_script_dict[script])
    else:
        with ZipFile(buffer, "w") as archive:
            for script in snowflake_dcr.cleaned_script_dict.keys():
                # Remove path
                script_name = script.lstrip("/")

                # Add number
                script_name = str(script_number) + " - " + script_name
                script_number += 1

                # write to archive
                archive.writestr(script_name, snowflake_dcr.cleaned_script_dict[script])


path = os.getcwd() + "/data-clean-room/"

with st.expander("Version Information"):
    for record in get_version_info(path):
        st.write(record)


# Build form based on selected action
if action == "Initial Deployment 🐻‍❄":
    # Form for initial deployment
    st.subheader("❄️ Initial DCR Deployment! ❄️")

    with st.form("initial_deployment_form"):
        dcr_version = "DCR 5.5 General Availability"
        abbreviation = st.text_input("What database abbreviation would you like? (Leave blank for default)")
        provider_account = st.text_input(label="What is the Provider's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        consumer_account = st.text_input(label="What is the Consumer's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        dcr_data_selection = st.selectbox("Would you like to load demo data?", dcr_data_options)
        include_comments = st.checkbox("Include comments in scripts", True)

        submitted = st.form_submit_button("Run")

        if submitted:
            if provider_account == consumer_account:
                st.error("Provider and consumer cannot be the same account!")
            else:
                with st.spinner("Generating Clean Room Scripts..."):
                    data_clean_room.prepare_dcr_deployment(True, dcr_version, provider_account, None, consumer_account,
                                                           None, abbreviation, path, dcr_data_selection)
                    data_clean_room.execute()

                # Message dependent on debug or not
                st.success("Scripts Ready for Download!")

                # Populate zip buffer for download buttons
                load_zip_buffer(data_clean_room, zip_buffer, include_comments)
                st.snow()

elif action == "Add Add'l Consumer 🐧️":
    # Form for adding consumers
    st.subheader("❄️ Add Consumers to Existing DCRs! ❄️")
    with st.form("additional_consumer_form"):
        dcr_version = "DCR 5.5 General Availability"
        abbreviation = st.text_input("What database abbreviation does the Provider have? (Leave blank for default)")
        provider_account = st.text_input(label="What is the existing Provider's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        consumer_account = st.text_input(label="What is the new Consumer's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        dcr_data_selection = st.selectbox("Would you like to load demo data?", dcr_data_options)
        include_comments = st.checkbox("Include comments in scripts", True)

        submitted = st.form_submit_button("Run")

        if submitted:
            if provider_account == consumer_account:
                st.error("Provider and consumer cannot be the same account!")
            else:
                path = os.getcwd() + "/data-clean-room/"

                with st.spinner("Generating Clean Room Scripts..."):
                    data_clean_room.prepare_consumer_addition(True, dcr_version, provider_account,
                                                              None, consumer_account, None,
                                                              abbreviation, path, dcr_data_selection)
                    data_clean_room.execute()

                st.success("Scripts Ready for Download!")

                # Populate zip buffer for download buttons
                load_zip_buffer(data_clean_room, zip_buffer, include_comments)
                st.snow()

elif action == "Add Add'l Provider ☃️":
    # Form for adding providers
    st.subheader("❄️ Add Providers to Existing DCRs! ❄️")
    with st.form("additional_provider_form"):
        dcr_version = "DCR 5.5 General Availability"
        abbreviation = st.text_input("What database abbreviation does the Consumer use? (Leave blank for default)")
        consumer_account = st.text_input(label="What is the existing Consumer's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        provider_account = st.text_input(label="What is the new Provider's account identifier?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        app_suffix = st.text_input("What suffix would you like for the Consumer-side app name? (Leave blank for "
                                   "default)")
        dcr_data_selection = st.selectbox("Would you like to load demo data?", dcr_data_options)
        include_comments = st.checkbox("Include comments in scripts", True)

        submitted = st.form_submit_button("Run")

        if submitted:
            if provider_account == consumer_account:
                st.error("Provider and consumer cannot be the same account!")
            else:
                path = os.getcwd() + "/data-clean-room/"

                with st.spinner("Generating Clean Room Scripts..."):
                    data_clean_room.prepare_provider_addition(True, dcr_version, provider_account,
                                                              None, consumer_account, None,
                                                              abbreviation, app_suffix, path, dcr_data_selection)
                    data_clean_room.execute()

                st.success("Scripts Ready for Download!")

                # Populate zip buffer for download buttons
                load_zip_buffer(data_clean_room, zip_buffer, include_comments)
                st.snow()

elif action == "Uninstall 💧":
    # Form for uninstalling
    st.subheader("❄️ Uninstall Existing DCRs! ❄️")
    st.warning("The scripts from this action **drop** related shares and databases!")
    with st.form("initial_deployment_form"):
        dcr_version = "DCR 5.5 General Availability"
        abbreviation = st.text_input(
            "What database abbreviation would you like to uninstall? (Leave blank for default)")
        account = st.text_input(label="What is the account identifier to uninstall?",
                                help="This should be just the account locator.  Anything beyond a '.' will be "
                                     "removed automatically.")
        account_type = st.selectbox("What is the account type for the account being uninstalled?", ["Consumer",
                                                                                                    "Provider"])

        consumer_account = st.text_input(label="What Consumer account identifier (if uninstalling Provider)?",
                                         help="This should be just the account locator.  Anything beyond a '.' will be "
                                              "removed automatically.")
        app_suffix = st.text_input("What suffix would you like for the Consumer-side app name? (Leave blank for "
                                   "default)", help="This is only relevant for multi-provider setups.")
        include_comments = st.checkbox("Include comments in scripts", True)

        st.warning("Running the generated scripts will **drop** related shares and databases!")
        submitted = st.form_submit_button("Run")

        if submitted:
            if account == consumer_account and account_type == "Provider":
                st.error("Provider and Consumer cannot be the same account!")
            else:
                path = os.getcwd() + "/data-clean-room/"

                with st.spinner("Generating Clean Room Scripts..."):
                    data_clean_room.prepare_uninstall(True, dcr_version, account_type, account, None,
                                                      consumer_account, abbreviation, app_suffix, path)
                    data_clean_room.execute()

                st.success("Scripts Ready for Download!")

                # Populate zip buffer for download buttons
                load_zip_buffer(data_clean_room, zip_buffer, include_comments)
                st.snow()

st.write("Once successfully run, please download your scripts!")
st.download_button(label="Download Scripts", data=zip_buffer, file_name="dcr_scripts.zip")
