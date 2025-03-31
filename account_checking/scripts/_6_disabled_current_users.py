import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def disabled_current_users(graph_headers, sql_connection):
    global disabled_current_users

    logging.info("Current employees with disabled accounts")

    employees_list = """Select BSq_Email, Sage_Name
                    FROM [Reports].[CentralSupport].[current_users]"""
    employees_list = pd.read_sql_query(employees_list, sql_connection)

    employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

    disabled_current_users = pd.DataFrame()

    for index, row in employees_list.iterrows():
        try:
            user_profile = f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}"

            user_profile = requests.get(user_profile, headers=graph_headers)

            if user_profile.status_code == 200:
                user_profile = user_profile.json()

                if user_profile['accountEnabled'] == False:
        
                    logging.info(f"{row['Sage_Name']} has an disabled account")
                   
                    user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], user_profile['accountEnabled']]],columns=['Name','Email','Enabled'])

                    #Add the user_data to the user_licences dataframe
                    disabled_current_users = pd.concat([disabled_current_users,user_data])

        except Exception as e:
            logging.error(f"Error: {e}")
            # pass

    if len(disabled_current_users) > 0:
        logging.info(f"Current employees with disabled accounts: {disabled_current_users}")