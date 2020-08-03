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
            positions_df = pd.DataFrame.from_records(positions_day)[['tradingsymbol', 'pnl', 'quantity', 'buy_price', 'sell_price', 'last_price', 'product']]
            self.logger.info("Fetched positions of orders placed today")
            self.mailer.send_mail("Needle : Positions Update (Net PnL : {})".format(round(positions_df.pnl.sum(), 2)), "Positions : <br>" + positions_df.to_html())
            self.mailer.send_mail('Needle : Orders Update', "All orders list : <br>" + pd.DataFrame(self.kite.orders()).to_html())
        except Exception as ex:
            self.logger.error('Error in fetching positions of orders placed today : {}'.format(ex))
            
if __name__ == "__main__":
    
    PositionsUpdate().execute()