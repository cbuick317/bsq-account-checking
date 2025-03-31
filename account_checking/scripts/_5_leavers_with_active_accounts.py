import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def leavers_with_active_accounts(graph_headers, sql_connection):
    global active_leavers

    logging.info("Leavers with active accounts")

    employees_list = """Select BSq_Email, Sage_Name, Employment_End_Date
                    FROM [Reports].[CentralSupport].[master_users] as MU

                    WHERE Employment_Status = 'Left' AND BSq_Email IS NOT NULL AND Employment_End_Date >= DATEADD(day, -180, GETDATE())

                    AND NOT EXISTS (SELECT BSq_Email FROM [Reports].[CentralSupport].current_users as CU where MU.BSq_Email = CU.BSq_Email)"""
    employees_list = pd.read_sql_query(employees_list, sql_connection)

    employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

    active_leavers = pd.DataFrame()

    for index, row in employees_list.iterrows():
        try:
            user_profile = f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}"

            user_profile = requests.get(user_profile, headers=graph_headers)

            if user_profile.status_code == 200:
                user_profile = user_profile.json()

                if user_profile['accountEnabled'] == True:
        
                    logging.info(f"{row['BSq_Email']} has an enabled account")

                    user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Employment_End_Date'], user_profile['accountEnabled']]],columns=['Name','Email', 'Employment End Date','Enabled'])

                    #Add the user_data to the user_licences dataframe
                    active_leavers = pd.concat([active_leavers,user_data])

        except Exception as e:
            logging.error(f"Error: {e}")
            # pass

    if len(active_leavers) > 0:
        logging.info(f"Leavers witha an active account: {active_leavers}")