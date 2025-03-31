import requests, pyodbc, pandas as pd, json

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def graph_token():
    global graph_headers

    credentials = {"grant_type": "client_credentials",
            "client_id": "a95d85cb-a194-4dfd-8324-e814490af1fe",
            "client_secret": "rOS8Q~5xYCuvzjf2wsOdXsAm1Frli9_awGwEqbgq",
            "resource": "https://graph.microsoft.com",}

    token = requests.get("https://login.microsoftonline.com/1e494fd4-7149-432f-9930-307b0073f993/oauth2/token", data=credentials)

    graph_headers = {"Authorization": "Bearer {}".format(token.json().get("access_token")),
                        'Content-Type': 'application/json'}
    
def mssql_connection():
    global sql_connection, cursor, engine, fast_engine
    
    sqlServer = "tcp:bs-sql-01.public.349085f154b3.database.windows.net,3342"
    sqlDriver= '{ODBC Driver 18 for SQL Server}'

    connection_string = ('Driver={ODBC Driver 18 for SQL Server}'+
                    ';Server=tcp:bs-sql-01.public.349085f154b3.database.windows.net,3342'+
                    ';Database=Reports'
                    ';Uid=dataadmin'
                    ';Pwd=Vuma1496'
                    ';Encrypt=yes'+
                    ';TrustServerCertificate=no'+
                    ';Connection Timeout=30;')

    sql_connection = pyodbc.connect(connection_string)           
    cursor = pyodbc.connect(connection_string).cursor()
    parameters = parse.quote_plus(connection_string)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % parameters)
    fast_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % parameters,fast_executemany=True)