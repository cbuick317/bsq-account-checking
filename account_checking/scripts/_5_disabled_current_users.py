import requests, pandas as pd, json, logging

#Disabled current users
def disabled_current_users(graph_headers, sql_connection):

    try:
        logging.info("Current employees with disabled accounts")

        #SQL query to retrieve list of users
        employees_list_query = """Select BSq_Email, Sage_Name
                        FROM [Reports].[CentralSupport].[current_users]
                        """
        
        employees_list = pd.read_sql_query(employees_list_query, sql_connection)

        #Convert the list of users emails to lowercase
        employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

        #Creates an empty dataframe to store the disabled current users data
        disabled_current_users = pd.DataFrame()

        #Loop through the list of users in disabled_current_users
        for index, row in employees_list.iterrows():
            try:
                user_profile = requests.get(f"https://graph.microsoft.com/beta/users/{row['BSq_Email']}", headers=graph_headers)

                if user_profile.status_code == 200:

                    if user_profile.json()['accountEnabled'] == False:
            
                        logging.info(f"{row['Sage_Name']} has an disabled account")
                    
                        user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], user_profile.json()['accountEnabled']]],columns=['Name','Email','Enabled'])

                        #Add the user_data to the user_licences dataframe
                        disabled_current_users = pd.concat([disabled_current_users,user_data])

            except Exception as err:
                    logging.error('There was an error retrieving information for '+str(row['Sage_Name'])+'%s', err)

        if len(disabled_current_users) > 0:
            logging.info(f"Current employees with disabled accounts: {disabled_current_users}")
        
        return disabled_current_users

    ##Send failure notification email if there is an error
    except Exception as err:
        from ...notifications._N_failure_notification_email import failure_notification
        logging.error('There was an error retrieving disabled current users data: %s', err)
        failure_notification(err, graph_headers,messageSubject="Error retrieving disabled current users data",messageBody="There was an retrieving disabled current users data")

        from ...functions.terminate_process import terminate_process_using_port
        terminate_process_using_port()
        
        return None