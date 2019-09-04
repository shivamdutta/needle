import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PositionsUpdate:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
            
        try:
            self.logger.debug("Fetching positions of orders placed today")
            positions = self.kite.positions()
            positions_day = positions['day']
            positions_df = pd.DataFrame.from_records(positions_day)
            self.logger.info("Fetched positions of orders placed today")
            self.mailer.send_mail('Needle : Positions Update', "Positions : <br>" + positions_df.to_html())   
        except Exception as ex:
            self.logger.error('Error in fetching positions of orders placed today : {}'.format(ex))
            
if __name__ == "__main__":
    
    PositionsUpdate().execute()