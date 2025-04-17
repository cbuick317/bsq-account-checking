import requests, pandas as pd, json, logging

#Leavers with active accounts
def leavers_with_active_accounts(graph_headers, sql_connection):

    try:
        logging.info("Leavers with active accounts")

        #SQL query to retrieve list of users
        employees_list_query = """Select BSq_Email, Sage_Name, Employment_End_Date
                        FROM [Reports].[CentralSupport].[master_users] as MU

                        WHERE Employment_Status = 'Left' AND BSq_Email IS NOT NULL AND Employment_End_Date >= DATEADD(day, -180, GETDATE())

                        AND NOT EXISTS (SELECT BSq_Email FROM [Reports].[CentralSupport].current_users as CU where MU.BSq_Email = CU.BSq_Email)
                        """
        
        employees_list = pd.read_sql_query(employees_list_query, sql_connection)

        #Convert the list of users emails to lowercase
        employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

        #Creates an empty dataframe to store the user leavers data
        active_leavers = pd.DataFrame()

        #Loop through the list of users in active_leavers
        for index, row in employees_list.iterrows():
            try:
                #Determine Graph API endpoint to retrieve user profile information
                user_profile = requests.get(f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}", headers=graph_headers)

                #If user account found
                if user_profile.status_code == 200:
                    
                    #If user account is enabled
                    if user_profile.json()['accountEnabled'] == True:
            
                        logging.info(f"{row['BSq_Email']} has an enabled account")

                        user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Employment_End_Date'], user_profile.json()['accountEnabled']]],columns=['Name','Email', 'Employment End Date','Enabled'])

                        #Add the user_data to the active_leavers dataframe
                        active_leavers = pd.concat([active_leavers, user_data])

            except Exception as err:
                logging.error('There was an error retrieving information for '+str(row['Sage_Name'])+'%s', err)

        if len(active_leavers) > 0:
            logging.info(f"Leavers with an active account: {active_leavers}")
        
        return active_leavers

    ##Send failure notification email if there is an error
    except Exception as err:
        from ...notifications._N_failure_notification_email import failure_notification
        logging.error('There was an error retrieving active leavers data: %s', err)
        failure_notification(err, graph_headers,messageSubject="Error retrieving active leavers data",messageBody="There was an retrieving active leavers data")

        from ...functions.terminate_process import terminate_process_using_port
        terminate_process_using_port()
        
        return None    