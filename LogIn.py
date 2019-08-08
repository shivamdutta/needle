from kiteconnect import KiteConnect
import json

from LoggerWrapper import Logger
from automate import Automate

class LogIn:
    
    def __init__(self):
        with open('config.json') as f:
            self.config = json.load(f)        
        self.logger = Logger('trades.log', 'INFO').logging
    
    def return_kite_obj(self):
        
        try:
            kite = KiteConnect(api_key=self.config['kite_api_key'])

            with open('auth.json') as f:
                auth = json.load(f)

            kite.set_access_token(auth['access_token'])
            self.logger.info('Logged in successfully')
            
        except Exception as ex:
            self.logger.error('First attempt to login failed : {}'.format(ex))
            try:
                self.logger.info('Trying to generate auth')
                auth = Automate().generate_auth()
                kite = KiteConnect(api_key=self.config['kite_api_key'])
                kite.set_access_token(auth['access_token'])
                self.logger.info('Logged in successfully')
                
            except Exception as ex:
                self.logger.error('Final attempt to login failed : {}'.format(ex))
                
        return kite