import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def future_occupancies_assigned_licences(graph_headers, sql_connection):
    global future_occupancies_assigned_licences_data
    
    employees_list = """Select BSq_Email, [Occupancy_Start_Date], Sage_Name FROM [Reports].[CentralSupport].[future_and_current_users] WHERE Employment_Status = 'Future Occupancy' AND Start_Date <= DATEADD(day, 7, GETDATE())"""
    employees_list = pd.read_sql_query(employees_list, sql_connection)

    employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

    future_occupancies_assigned_licences_data = pd.DataFrame()

    #SQL query to retrieve list of licences from the licenceReference table
    licence_library = "SELECT DISTINCT [licenceSKU] ,[licenceName] ,[skuID] FROM [Microsoft].[dbo].[licenceReference] ORDER BY licenceSKU" 
    licence_library = pd.read_sql_query(licence_library, sql_connection)

    #Creates an empty dataframe to store the user licence data
    user_licences = pd.DataFrame()

    for index, row in employees_list.iterrows():
        try:
            user_profile = requests.get("https://graph.microsoft.com/beta/users/"+str(row['BSq_Email']), headers=graph_headers).json()

            licences = []

            #Loop through the list of licences assigned to the user
            for licence in user_profile['assignedLicenses']:

                #Get the friendly name for the licence from the licenceReference table
                licenceInfo = licence_library.loc[licence_library['skuID'] == licence['skuId'], 'licenceName']

                #Append the licence name to the licences list
                licences.append(str(licenceInfo.iloc[0]))

            user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Occupancy_Start_Date'], user_profile['accountEnabled'], licences]],columns=['Name','Email', 'Start Date','Enabled','Active Licences'])

            future_occupancies_assigned_licences_data = pd.concat([future_occupancies_assigned_licences_data,user_data])
        except Exception as e:
                logging.error(f"Error: {e}")
                # pass

    if len(future_occupancies_assigned_licences_data) > 0:
        logging.info(f"Future employees licence information: {future_occupancies_assigned_licences_data}")