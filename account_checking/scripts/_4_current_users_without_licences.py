import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def check_all_values_missing(nested_list, values_to_check):

    found_count = 0
    for value in values_to_check:
        for dictionary in nested_list:
            if value in dictionary.values():
                found_count += 1
                break # break the inner loop when a value is found in a dictionary, preventing multiple counts.
        else: #else statement of the for loop, executes if the inner for loop completes without break
            continue # if the value is found, continue to the next value
        
    if found_count == len(values_to_check):
        return "all_found"
    elif found_count > 0:
        return "some_found"
    else:
        return "none_found"

#Current users with no licences assigned
def current_users_without_licences(graph_headers, sql_connection):
    global current_user_licences

    logging.info("Current users with no licences, no E3 or F3 licences assigned, or an E3 and F3 licence assigned")

    #SQL query to retrieve list of users
    employees_list = """Select BSq_Email, Sage_Name
                        FROM [Reports].[CentralSupport].[current_users] as CU

						WHERE BSq_Email NOT IN ('jolene.atherton@bluesquare.uk.com','hilary.leaver@bluesquare.uk.com')
						ORDER BY Sage_Name"""
    
    employees_list = pd.read_sql_query(employees_list, sql_connection)

    #Convert the list of users emails to lowercase
    employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

    #SQL query to retrieve list of licences from the licenceReference table
    licence_library = "SELECT DISTINCT [licenceSKU] ,[licenceName] ,[skuID] FROM [Microsoft].[dbo].[licenceReference] ORDER BY licenceSKU" 
    licence_library = pd.read_sql_query(licence_library, sql_connection)

    #Creates an empty dataframe to store the user licence data
    current_user_licences = pd.DataFrame()

    #Loop through the list of users in employees_List
    for index, row in employees_list.iterrows():
        try:
            #Determine Graph API endpoint to retrieve user profile information
            user_profile = requests.get(f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}", headers=graph_headers)

            #If user account found
            if user_profile.status_code == 200:
                user_profile = user_profile.json()
                
                #If user has licences assigned i.e. is not an empty list
                if user_profile['assignedLicenses'] == []:
                    
                    logging.info(f"{row['Sage_Name']} has no licences assigned")
                    
                    user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], user_profile['accountEnabled'], 'No licences assigned']],columns=['Name','Email','Enabled','Licences'])

                    #Add the user_data to the user_licences dataframe
                    current_user_licences = pd.concat([current_user_licences,user_data])

                else:
                    licence_skus = ['4b585984-651b-448a-9e53-3b10f069cf7f','6fd2c87f-b296-42f0-b197-1e91e994b900']
                  
                    if check_all_values_missing(user_profile['assignedLicenses'], licence_skus) == "none_found":
                        logging.info(f"{row['Sage_Name']} has neither an F3 or E3 licence.")
                                
                        #Create a user_data dataframe with the user profile information and the licences assigned
                        user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], user_profile['accountEnabled'], 'No E3 or F3 licences assigned']],columns=['Name','Email','Enabled','Licences'])

                        #Add the user_data to the user_licences dataframe
                        current_user_licences = pd.concat([current_user_licences,user_data])

                    elif check_all_values_missing(user_profile['assignedLicenses'], licence_skus) == "all_found":
                        logging.info(f"{row['Sage_Name']} has both an F3 and an E3 licence.")

                        #Create a user_data dataframe with the user profile information and the licences assigned
                        user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], user_profile['accountEnabled'], 'E3 and F3 licences assigned']],columns=['Name','Email','Enabled','Licences'])

                        #Add the user_data to the user_licences dataframe
                        current_user_licences = pd.concat([current_user_licences,user_data])


        except Exception as e:
            logging.error(f"Error: {e}")

    if len(current_user_licences) > 0:
        logging.info(f"Users with no licences assigned or missing an F3 or E3 licence: {current_user_licences}")