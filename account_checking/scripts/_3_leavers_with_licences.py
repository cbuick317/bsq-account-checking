import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

#Leavers and those more than 7 days from now who have an E3 or F3 licence assigned

def leavers_with_licences(graph_headers, sql_connection):
    global leavers_and_future_starters_user_licences

    logging.info("Leavers and those starting more than 7 days from now who have an E3 or F3 licence assigned")

    #SQL query to retrieve list of users
    employees_list = """Select BSq_Email ,Sage_Name, Start_Date, Employment_End_Date
                        FROM [Reports].[CentralSupport].[master_users] as MU

                        WHERE Employment_Status = 'Left' AND BSq_Email IS NOT NULL AND Employment_End_Date >= DATEADD(day, -180, GETDATE())

                        AND NOT EXISTS (SELECT BSq_Email FROM [Reports].[CentralSupport].current_users as CU where MU.BSq_Email = CU.BSq_Email)

                        UNION ALL 

                        SELECT BSq_Email, Sage_Name, Occupancy_Start_Date as Start_Date, Employment_End_Date
                        FROM [Reports].[CentralSupport].future_and_current_users as FCU
                        WHERE Employment_Status  = 'Future Occupancy'
                        AND Start_Date >= DATEADD(day, 7, GETDATE())"""
    
    employees_list = pd.read_sql_query(employees_list, sql_connection)

    #Convert the list of users emails to lowercase
    employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

    #SQL query to retrieve list of licences from the licenceReference table
    licence_library = "SELECT DISTINCT [licenceSKU] ,[licenceName] ,[skuID] FROM [Microsoft].[dbo].[licenceReference] ORDER BY licenceSKU" 
    licence_library = pd.read_sql_query(licence_library, sql_connection)

    #Creates an empty dataframe to store the user licence data
    leavers_and_future_starters_user_licences = pd.DataFrame()

    #Loop through the list of users in employees_List
    for index, row in employees_list.iterrows():
        try:
            #Determine Graph API endpoint to retrieve user profile information
            user_profile = requests.get(f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}", headers=graph_headers)

            #If user account found
            if user_profile.status_code == 200:
                user_profile = user_profile.json()
                
                #If user has licences assigned i.e. is not an empty list
                if user_profile['assignedLicenses'] != []:

                    #Create emoty list to store licence information
                    licences = []

                    #Loop through the list of licences assigned to the user
                    for licence in user_profile['assignedLicenses']:

                        #Get the friendly name for the licence from the licenceReference table
                        licenceInfo = licence_library.loc[licence_library['skuID'] == licence['skuId'], 'licenceName']

                        #Append the licence name to the licences list
                        licences.append(str(licenceInfo.iloc[0]))

                    logging.info(f"{row['Sage_Name']} has the following licences: {licences}")

                    #Create a user_data dataframe with the user profile information and the licences assigned
                    user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Start_Date'], row['Employment_End_Date'], user_profile['accountEnabled'], licences]],columns=['Name','Email', 'Start Date', "End Date",'Enabled','Licences'])

                    #Add the user_data to the user_licences dataframe
                    leavers_and_future_starters_user_licences = pd.concat([leavers_and_future_starters_user_licences,user_data])

        except Exception as e:
            logging.error(f"Error: {e}")

    if len(leavers_and_future_starters_user_licences) > 0:
        logging.info(f"Users with licences assigned: {leavers_and_future_starters_user_licences}")