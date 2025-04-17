import requests, pyodbc, pandas as pd, json, logging

def main():
    from ...access.access import graph_token, ms_sql_connection
    graph_headers = graph_token()
    ms_sql_connection('Reports')

    from ...access.access import sql_connection

    #Retrieve the licence library from SQL
    from ._1_licence_library import licence_library
    licence_library = licence_library(graph_headers, sql_connection)

    # Leavers and those starting outside next 7 days who have an E3 or F3 licence assigned
    from._2_leavers_with_licences import leavers_with_licences
    leavers_and_future_starters_user_licences = leavers_with_licences(graph_headers, sql_connection, licence_library)

    # Alert for any current_users without an O365 E3 or O365 F3 licence assigned.
    from ._3_current_users_without_licences import current_users_without_licences
    current_user_licences = current_users_without_licences(graph_headers, sql_connection)

    #Users who have left the business but still have an active account in Azure
    from ._4_leavers_with_active_accounts import leavers_with_active_accounts
    active_leavers = leavers_with_active_accounts(graph_headers, sql_connection)

    #Current users who have a disabled account in Azure
    from ._5_disabled_current_users import disabled_current_users
    disabled_current_users = disabled_current_users(graph_headers, sql_connection)
  
    #New employees starting in next 7 days and assigned licences
    from ._6_future_occupancies_assigned_licences import future_occupancies_assigned_licences
    future_occupancies_assigned_licences_data = future_occupancies_assigned_licences(graph_headers, sql_connection, licence_library)
   
    #Notification email sent to Service Desk
    from ._8_notification_email import notification_email
    notification_email(graph_headers, leavers_and_future_starters_user_licences, current_user_licences, active_leavers, disabled_current_users, future_occupancies_assigned_licences_data)