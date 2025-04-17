import requests, pyodbc, pandas as pd, json, logging

#Leavers and those more than 7 days from now who have an E3 or F3 licence assigned
def licence_library(graph_headers, sql_connection):

    try:
        #SQL query to retrieve list of licences from the licenceReference table
        licence_library_query = "SELECT DISTINCT [licenceSKU] ,[licenceName] ,[skuID] FROM [Microsoft].[dbo].[licenceReference] ORDER BY licenceSKU" 
        licence_library = pd.read_sql_query(licence_library_query, sql_connection)

        return licence_library

    ##Send failure notification email if there is an error
    except Exception as err:
        from ...notifications._N_failure_notification_email import failure_notification
        logging.error('There was an error retrieving the licence library data from SQL: %s', err)
        failure_notification(err, graph_headers,messageSubject="Error retrieving data from SQL",messageBody="There was an error retrieving data There was an error retrieving the licence library data from SQL")

        from ...functions.terminate_process import terminate_process_using_port
        terminate_process_using_port()
        
        return None