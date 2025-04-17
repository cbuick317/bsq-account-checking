import requests, pandas as pd, json, logging

#Future occupancies assigned licences
def future_occupancies_assigned_licences(graph_headers, sql_connection, licence_library):

    try:
        logging.info("Future occupancies with assigned licences")

        #SQL query to retrieve list of users
        employees_list_query = """Select BSq_Email, [Occupancy_Start_Date], Sage_Name 
                            FROM [Reports].[CentralSupport].[future_and_current_users] 
                            WHERE Employment_Status = 'Future Occupancy' 
                            AND Start_Date <= DATEADD(day, 7, GETDATE())
                        """
        
        employees_list = pd.read_sql_query(employees_list_query, sql_connection)

        #Convert the list of users emails to lowercase
        employees_list['BSq_Email'] = employees_list['BSq_Email'].str.lower()

        #Creates an empty dataframe to store the future occupancies assigned licences data
        future_occupancies_assigned_licences_data = pd.DataFrame()

        #Creates an empty dataframe to store the user licence data
        user_licences = pd.DataFrame()

        #Loop through the list of users in future_occupancies_assigned_licences
        for index, row in employees_list.iterrows():
            try:
                user_profile = requests.get("https://graph.microsoft.com/beta/users/"+str(row['BSq_Email']), headers=graph_headers).json()

                licences = []

                #Loop through the list of licences assigned to the user
                for licence in user_profile['assignedLicenses']:

                    #Append the licence name to the licences list
                    licences.append(str(licence_library.loc[licence_library['skuID'] == licence['skuId'], 'licenceName'].iloc[0]))

                user_data = pd.DataFrame([[row['Sage_Name'], row['BSq_Email'], row['Occupancy_Start_Date'], user_profile['accountEnabled'], licences]],columns=['Name','Email', 'Start Date','Enabled','Active Licences'])

                future_occupancies_assigned_licences_data = pd.concat([future_occupancies_assigned_licences_data,user_data])

            except Exception as err:
                logging.error('There was an error retrieving licence information for '+str(row['Sage_Name'])+'%s', err)

        if len(future_occupancies_assigned_licences_data) > 0:
            logging.info(f"Future employees licence information: {future_occupancies_assigned_licences_data}")
            
        return future_occupancies_assigned_licences_data

    ##Send failure notification email if there is an error
    except Exception as err:
        from ...notifications._N_failure_notification_email import failure_notification
        logging.error('There was an error retrieving future occupancies assigned licences data: %s', err)
        failure_notification(err, graph_headers,messageSubject="Error retrieving future occupancies assigned licences data",messageBody="There was an retrieving future occupancies assigned licences data")

        from ...functions.terminate_process import terminate_process_using_port
        terminate_process_using_port()
        
        return None