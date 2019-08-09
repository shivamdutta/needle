import pandas as pd
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
import json

from LoggerWrapper import Logger

class Mailer:
    
    def __init__(self):
        with open('config.json') as f:
            self.config = json.load(f)
        self.logger = Logger('trades.log', 'INFO').logging
        self.logger.getLogger('googleapiclient.discovery_cache').setLevel(self.logger.CRITICAL)
        self.logger.getLogger('googleapiclient.discovery').setLevel(self.logger.CRITICAL)
        
    def load_gmail_service(self):
        """Shows basic usage of the Gmail API.
        Lists the user's Gmail labels.
        """
        creds = None

        # If modifying these scopes, delete the file token.pickle.
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.send']

        # The file token.pkl stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.

        if os.path.exists('token.pkl'):
            with open('token.pkl', 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(host='localhost',port=9090)

            # Save the credentials for the next run
            with open('token.pkl', 'wb') as token:
                pickle.dump(creds, token)

        service = build('gmail', 'v1', credentials=creds)

        # Call the Gmail API
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])

        return service
    
    def send_mail(self, subject="No Args Error", message_text="No Args Error"):
        
        if self.config['send_mails']:
            try:
                message = MIMEText(message_text, 'html')
                message['to'] = self.config['mail_to']
                message['from'] = self.config['mail_from']
                message['subject'] = subject

                msg = {'raw': base64.urlsafe_b64encode(message.as_string().encode())}
                msg['raw'] = msg['raw'].decode("utf-8")

                user_id='me'
                service = self.load_gmail_service()

                message_return = service.users().messages().send(userId=user_id, body=msg).execute()
                self.logger.debug('Email sent')
            except Exception as ex:
                self.logger.error('Error in sending email : {}'.format(ex))
                return False
            
            return True
        else:
            return False
        
if __name__ == "__main__":
    
    message = Mailer().send_mail("Needle : Trade Update", "Testing mailing system. Please ignore.")