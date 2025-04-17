import json, requests

def failure_notification(err, graph_headers, messageSubject, messageBody):
    
    #Failure email definition
    email = json.dumps(
        {
            "message": 
                {
                    "subject": "Error - "+ str(messageSubject),
                    "body": 
                        {
                            "contentType": "HTML",
                            "content": str(messageBody)+"<br/>\n<br/>\n"+str(err),
                        },
                    "toRecipients": [
                        {
                            "emailAddress":
                            {
                                "address": "cbuick@bluesquare.uk.com"
                            }
                        }
                    ],
                }, 
                "saveToSentItems": "true"
        })

    requests.request("POST","https://graph.microsoft.com/beta/users/dataadmin@bluesquare.uk.com/sendMail",headers=graph_headers, data=email )