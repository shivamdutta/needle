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
            self.logger.debug("Fetching orders placed today and their positions")

            orders_df = pd.DataFrame(self.kite.orders())
            orders_df = orders_df[orders_df['product']=='MIS'][['tradingsymbol', 'transaction_type', 'status', 'order_timestamp',  'order_type', 'tag', 'price', 'trigger_price', 'average_price', 'quantity']]

            positions_df = pd.DataFrame.from_records(self.kite.positions()['day'])
            positions_df = positions_df[positions_df['product']=='MIS'][['tradingsymbol', 'pnl', 'quantity', 'buy_price', 'sell_price', 'last_price']]

            self.mailer.send_mail("Needle : Positions & Orders Update (Net PnL : {})".format(round(positions_df.pnl.sum(), 2)), "Positions : <br>" + positions_df.to_html() + " <br> Orders : <br>" + orders_df.to_html())

            self.logger.info("Fetched orders placed today and their positions")
        except Exception as ex:
            self.logger.error('Error in fetching orders placed today and their positions : {}'.format(ex))
            
if __name__ == "__main__":
    
    PositionsUpdate().execute()