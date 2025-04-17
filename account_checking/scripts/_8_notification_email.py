import requests, pandas as pd, json, logging
from datetime import date

def notification_email(graph_headers, leavers_and_future_starters_user_licences, current_user_licences, active_leavers, disabled_current_users, future_occupancies_assigned_licences_data):

    leavers_and_future_starters_user_licences = "None" if leavers_and_future_starters_user_licences.empty else str(leavers_and_future_starters_user_licences.to_html(index = False))

    current_user_licences = "None" if current_user_licences.empty else str(current_user_licences.to_html(index = False))
 
    active_leavers = "None" if active_leavers.empty else str(active_leavers.to_html(index = False))
 
    disabled_current_users = "None" if disabled_current_users.empty else  str(disabled_current_users.to_html(index = False))
   
    future_occupancies_assigned_licences_data =  "None" if future_occupancies_assigned_licences_data.empty else  str(future_occupancies_assigned_licences_data.to_html(index = False))
   
    notification_email_definition = json.dumps(
         {
                    "message": 
                        {
                            "subject": "Daily Azure account checking  - "+(date.today()).strftime('%Y-%m-%d'),
                            
                            "body": 
                                {
                                    "contentType": "HTML",
                                    "content": "Hello,<br/>\n<br/>\nPlease see below any accounts that need to be reviewed for access<br/>\n<br/>\n"+
                                    
                                    "<b>Leavers and those starting more than 7 days from now who have an E3 or F3 licence assigned:</b><br/>\n\n"+
                                    leavers_and_future_starters_user_licences + "<br/>\n<br/>\n"+

                                    "<b>Current users with no licences, no E3 or F3 licences assigned, or both an E3 and F3 licence assigned:</b><br/>\n\n"+
                                    current_user_licences + "<br/>\n<br/>\n"+

                                    "<b>Leavers with active accounts:</b><br/>\n\n"+
                                    active_leavers + "<br/>\n<br/>\n"+

                                    "<b>Current employees with disabled accounts:</b><br/>\n\n"+
                                    disabled_current_users + "<br/>\n<br/>\n"+

                                    "<b>Future occupancies (next 7 days):</b><br/>\n\n"+
                                    future_occupancies_assigned_licences_data + "<br/>\n<br/>\n"
                                    
                                },
                            "toRecipients": [{"emailAddress":{"address": "servicedesk@bluesquare.uk.com"}}],
                        }, 
                    "saveToSentItems": "true"
                })
    
    response = requests.request("POST","https://graph.microsoft.com/beta/users/dataadmin@bluesquare.uk.com/sendMail",headers=graph_headers, data=notification_email_definition )
    logging.info(f"Email sent {str(response.status_code)}")