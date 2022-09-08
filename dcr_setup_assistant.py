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

# Create dcr object
data_clean_room = dcr.SnowflakeDcr()
zip_buffer = io.BytesIO()

# Form for initial deployment
st.subheader("❄️ Initial DCR Deployment! ❄️")
st.text("Generates scripts for deploying Data Clean Room version 5.5")
with st.form("initial_deployment_form"):
    dcr_version = "5.5 SQL Param"
    abbreviation = st.text_input("What database abbreviation would you like? (Leave blank for default)")
    provider_account = st.text_input(label="What is the Provider account identifier?",help="This should be just the "
                                                                                           "account locator.  Anything "
                                                                                           "beyond a '.' will be "
                                                                                           "removed automatically.")
    consumer_account = st.text_input(label="What is the Consumer account identifier?",help="This should be just the "
                                                                                           "account locator.  Anything "
                                                                                           "beyond a '.' will be "
                                                                                           "removed automatically.")
    include_comments = st.checkbox("Include comments in scripts", True)

    submitted = st.form_submit_button("Run")

    if submitted:
        if provider_account == consumer_account:
            st.error("Provider and consumer cannot be the same account!")
        else:
            path = os.getcwd() + "/data-clean-room/"

            with st.spinner("Deploying Clean Room..."):
                data_clean_room.prepare_deployment(True, dcr_version, provider_account, None,
                                                   consumer_account, None, abbreviation, path)
                data_clean_room.execute()

            # Message dependent on debug or not
            st.success("Scripts Ready for Download!")

            # Populate zip buffer for download buttons
            if include_comments:
                with ZipFile(zip_buffer, "w") as archive:
                    for script in data_clean_room.prepared_script_dict.keys():
                        archive.writestr(script, data_clean_room.prepared_script_dict[script])
            else:
                with ZipFile(zip_buffer, "w") as archive:
                    for script in data_clean_room.cleaned_script_dict.keys():
                        archive.writestr(script, data_clean_room.cleaned_script_dict[script])
            st.snow()

st.write("Once successfully run, please download your scripts!")
st.download_button(label="Download Scripts", data=zip_buffer, file_name="dcr_scripts.zip")
