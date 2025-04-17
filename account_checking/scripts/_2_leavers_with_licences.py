import requests, pyodbc, pandas as pd, json, logging

#Leavers and those more than 7 days from now who have an E3 or F3 licence assigned
def leavers_with_licences(graph_headers, sql_connection, licence_library):

    try:
        logging.info("Leavers and those starting more than 7 days from now who have an E3 or F3 licence assigned")

        #SQL query to retrieve list of users based on the criteria specified
        employees_list_query = """Select BSq_Email ,Sage_Name, Start_Date, Employment_End_Date
                            FROM [Reports].[CentralSupport].[master_users] as MU

                            WHERE Employment_Status = 'Left' AND BSq_Email IS NOT NULL AND Employment_End_Date >= DATEADD(day, -180, GETDATE())
                            AND NOT EXISTS (SELECT BSq_Email FROM [Reports].[CentralSupport].current_users as CU where MU.BSq_Email = CU.BSq_Email)

                            UNION ALL 

                            SELECT BSq_Email, Sage_Name, Occupancy_Start_Date as Start_Date, Employment_End_Date
                            FROM [Reports].[CentralSupport].future_and_current_users as FCU
                            
                            WHERE Employment_Status  = 'Future Occupancy'
                            AND Start_Date >= DATEADD(day, 7, GETDATE())
                        """
        
        #Read the SQL query into a pandas dataframe
        employees_list = pd.read_sql_query(employees_list_query, sql_connection)

        #Convert the list of users emails to lowercase
        employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

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

                            #Append the licence name to the licences list
                            licences.append(str(licence_library.loc[licence_library['skuID'] == licence['skuId'], 'licenceName'].iloc[0]))
                       
                        #Create a user_data dataframe with the user profile information and the licences assigned
                        user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Start_Date'], row['Employment_End_Date'], user_profile['accountEnabled'], licences]],columns=['Name','Email', 'Start Date', "End Date",'Enabled','Licences'])

                        #Add the user_data to the user_licences dataframe
                        leavers_and_future_starters_user_licences = pd.concat([leavers_and_future_starters_user_licences,user_data])

            except Exception as err:
                logging.error('There was an error retrieving licence information for '+str(row['Sage_Name'])+'%s', err)

        if len(leavers_and_future_starters_user_licences) > 0:
            logging.info(f"Leavers and those starting more than 7 days from now with an E3 or F3 licence assigned: {leavers_and_future_starters_user_licences}")
        
        return leavers_and_future_starters_user_licences

    ##Send failure notification email if there is an error
    except Exception as err:
        from ...notifications._N_failure_notification_email import failure_notification
        logging.error('There was an error retrieving retrieving leavers with licences data: %s', err)
        failure_notification(err, graph_headers,messageSubject="Error retrieving leavers with licences data",messageBody="There was an error retrieving leavers with licences data")

        from ...functions.terminate_process import terminate_process_using_port
        terminate_process_using_port()
        
        return None