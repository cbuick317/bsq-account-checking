import requests, pyodbc, pandas as pd, json, logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from urllib import parse
from datetime import date,datetime, timedelta

def main():
    from ._2_access import graph_token, mssql_connection
    graph_token(),mssql_connection()

    from ._2_access import graph_headers, sql_connection, cursor, engine, fast_engine

    # Leavers and those starting outside next 7 days who have an E3 or F3 licence assigned
    from._3_leavers_with_licences import leavers_with_licences
    leavers_with_licences(graph_headers, sql_connection)
    from ._3_leavers_with_licences import leavers_and_future_starters_user_licences

    # Alert for any current_users without an O365 E3 or O365 F3 licence assigned.
    from ._4_current_users_without_licences import current_users_without_licences
    current_users_without_licences(graph_headers, sql_connection)
    from ._4_current_users_without_licences import current_user_licences

    #Users who have left the business but still have an active account in Azure
    from ._5_leavers_with_active_accounts import leavers_with_active_accounts
    leavers_with_active_accounts(graph_headers, sql_connection)
    from ._5_leavers_with_active_accounts import active_leavers

    #Current users who have a disabled account in Azure
    from ._6_disabled_current_users import disabled_current_users
    disabled_current_users(graph_headers, sql_connection)
    from ._6_disabled_current_users import disabled_current_users

    #New employees starting in next 7 days and assigned licences
    from ._7_future_occupancies_assigned_licences import future_occupancies_assigned_licences
    future_occupancies_assigned_licences(graph_headers, sql_connection)
    from ._7_future_occupancies_assigned_licences import future_occupancies_assigned_licences_data

    from ._8_notification_email import notification_email
    notification_email(graph_headers, leavers_and_future_starters_user_licences, current_user_licences, active_leavers, disabled_current_users, future_occupancies_assigned_licences_data)