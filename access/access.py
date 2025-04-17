import logging, os, requests, json, azure.functions as func, pyodbc, urllib.parse

from azure.cosmos import CosmosClient
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse

#GRAPH TOKEN REQUEST SUB-FUNCTION
def graph_token():
    #Defines global headers that can be called via other sub-functions within the function
    global graph_headers

    try:
        logging.info("Retrieving Graph token...")
        
        #Defines the JSON payload with necessary credential information to establish an authorised connection to Graph.
        #Within Azure, these credentials are stored within key vaults and decrypted on execution
        credentials = {"grant_type": os.getenv("graphGrantType"),
                        "client_id": os.getenv("graphClientId"),
                        "client_secret": os.getenv("graphClientSecret"),
                        "resource": os.getenv("graphResource"),}

        #Sends request to token endpoint to retrieve access token
        token = requests.get(os.getenv("graphTokenUrl"), data=credentials)

        #Uses retrieved access token to establish variable containing Graph headers required for subsequent Graph API calls
        graph_headers = {"Authorization": "Bearer {}".format(token.json().get("access_token")),
                         'Content-Type': 'application/json'}
        
        logging.info("Graph token retrieved successfully")
    
        return graph_headers

    ##Send failure notification email if there is an error retrieving the Graph token
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error retrieving Graph token" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error retrieving Graph token",messageBody="Error retrieving Graph token")

#CENTRAL HUB API HEADERS DEFINITION SUB-FUNCTION
def central_hub_token():
    #Defines global headers that can be called via other sub-functions within the function
    global central_hub_auth, central_hub_headers
    
    try:
        logging.info("Retrieving Central Hub token...")

        #Defines variables for Central Hub authentication and headers required to make API request to Central Hub related endpoints.
        #Within Azure, these credentials are stored within key vaults and decrypted on execution
        central_hub_auth = (os.getenv("centralhubauth"), "")
        central_hub_headers = {'Content-Type' : 'application/json'}

        logging.info("Central Hub token retrieved successfully")

    ##Send failure notification email if there is an error retrieving the Central Hub token
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error retrieving Central Hub token" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error retrieving Central Hub token",messageBody="Error retrieving Central Hub token")
        return
    
#TRIBEPAD TOKEN REQUEST SUB-FUNCTION
def tribepad_token():
    #Defines global headers that can be called via other sub-functions within the function
    global tribepad_auth_headers, credentials_payload

    try:
        logging.info("Retrieving Tribepad token...")

        #Defines the JSON payload with necessary credential information to establish an authorised connection to Tribepad.
        #Within Azure, these credentials are stored within key vaults and decrypted on execution
        credentials_payload = {'client_id': os.getenv('tribepadClientId'),
                                'client_secret': os.getenv('tribepadClientSecret'),
                                'grant_type': os.getenv('tribepadGrantType'),
                                'scope': os.getenv('tribepadScope')}
        
        #Defines the JSON payload with necessary header information to establish an authorised connection to Tribepad.
        tribepad_headers = {
        'Cookie': os.getenv("tribepadCookie")}

        #Sends request to token endpoint to retrieve access authorisation headers and stores header definition in payload variable
        tribepad_auth_headers = {
            'Content-Type': 'application/json',
            "Authorization": "Bearer "+requests.request("POST", os.getenv("tribepadTokenUrl"), headers= tribepad_headers, data=credentials_payload, files=[]).json()['access_token'],
            'Cookie': os.getenv("tribepadCookie")}

        logging.info("Tribepad token retrieved successfully")

    #Send failure notification email if there is an error retrieving the Tribepad token
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error(f"Error retrieving Tribepad token {err}")
        failure_notification(err, graph_headers,messageSubject="Error retrieving Central Hub token",messageBody="Error retrieving Tribepad token")
        return

#AZURE COSMOSDB CONNECTION SUB-FUNCTION
def azure_cosmos_connection():
    #Defines global headers that can be called via other sub-functions within the function
    global azure_cosmos_client

    try:
        #Defines the endpoint url and access key related to a specific Azure CosmosDB service to establish an authorised connection.
        #Within Azure, these credentials are stored within key vaults and decrypted on execution
        endpoint_uri = os.getenv("bsqonboardingCosmosEndpoint")
        primary_key = os.getenv("bsqonboardingCosmosKey")

        #Creates the Azure Cosmos Client necessary for subsequent Azure CosmosDB database process and stores it as a variable.
        azure_cosmos_client = CosmosClient(endpoint_uri, primary_key)

        logging.info("Azure Cosmos connection established successfully")
        
        return azure_cosmos_client

    ##Send failure notification email if there is an error connecting to Azure CosmosDB
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error establishing Azure Cosmos connection" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error establishing Azure Cosmos connection",messageBody="Error establishing Azure Cosmos connection")
        return

#MSSQL CONNECTION SUB-FUNCTION
def ms_sql_connection(database):
    #Defines global headers that can be called via other sub-functions within the function
    global sql_connection, cursor, engine, connection_string, fast_engine

    try:
        logging.info("Establishing SQL connection...")

        #Defines the JSON payload with necessary connection information to establish an authorised connection to MSSQL.
        connection_string = ('Driver='+os.getenv("sqlDriver")+
                            ';Server=' + os.getenv("sqlServer") +
                            ';Database=' + database +
                            ';Uid='+os.getenv("sqlUser")+
                            ';Pwd='+os.getenv("sqlPass")+
                            ';Encrypt=yes'+
                            ';TrustServerCertificate=no'+
                            ';Connection Timeout=30;')

        #Defines various variables required for MSSQL queries.
        sql_connection = pyodbc.connect(connection_string)       
        cursor = pyodbc.connect(connection_string).cursor()
        parameters = parse.quote_plus(connection_string)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % parameters)
        fast_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % parameters,fast_executemany=True)

        logging.info("SQL connection established successfully")

    ##Send failure notification email if there is an error connecting to SQL
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error establishing SQL connection" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error establishing SQL connection",messageBody="Error establishing SQL connection")
        return

#REWARD GATEWAY AUTHENTICATION SUB-FUNCTION
def reward_gateway_auth():
    global rg_headers

    try:
        logging.info("Retrieving Reward Gateway token...")

        rg_headers = {'Authorization': os.getenv("rgAuthorisation"),
                        'Cookie': os.getenv("rgCookie")}
        
        logging.info("Reward Gateway token retrieved successfully")

    ##Send failure notification email if there is an error retrieving the Reward Gateway token
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error retrieving Reward Gateway token" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error retrieving Reward Gateway token",messageBody="Error retrieving Reward Gateway token")
        return

#SAP TOKEN REQUEST SUB-FUNCTION    
def sap_token():
        
    try:
        logging.info("Retrieving SAP token...")

        #Get SAP access token using refresh token
        headers = {"Content-Type": "application/x-www-form-urlencoded",
                    "Connection": "close"}

        refresh_data = urllib.parse.urlencode({
            'client_id': os.getenv('sapClientID'),
            'client_secret': os.getenv('sapClientSecret'),
            'grant_type':os.getenv('sapGrantType'),
            'refresh_token': os.getenv('sapRefreshToken'),
        })

        response = requests.post(os.getenv('sapTokenUrl'), data=refresh_data, headers=headers)

        headers = {'Authorization': 'Bearer ' + response.json()['access_token'],
                    'Accept': 'application/json',    
                    'Content-Type': 'application/json'}
        
        logging.info("SAP token retrieved successfully")

    ##Send failure notification email if there is an error retrieving the Reward Gateway token
    except Exception as err:
        from ..notifications._N_failure_notification_email import failure_notification
        logging.error("Error retrieving SAP token" + str(err))
        failure_notification(err, graph_headers,messageSubject="Error retrieving SAP token",messageBody="Error retrieving SAP token")
        return