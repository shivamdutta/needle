import pandas as pd
import os
import os.path

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class RejectedOrders:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
        
        try:
            self.logger.debug("Fetching all orders")
            orders = self.kite.orders()
            orders_df = pd.DataFrame(orders)
            all_rejected_orders = orders_df[orders_df['status']=='REJECTED']
            self.logger.debug("Fetched all orders")
            
            try:
                self.logger.debug("Reading rejected orders")
                if os.path.exists('rejected_trades_today.csv'):
                    mailed_rejected_orders = pd.read_csv("rejected_trades_today.csv")
                else:
                    mailed_rejected_orders = pd.DataFrame(columns=orders_df.columns.tolist())
                
                rejected_orders_to_mail = all_rejected_orders[~(all_rejected_orders.order_id.isin(mailed_rejected_orders.order_id.unique().tolist()))]
                
                if len(rejected_orders_to_mail):
                    self.logger.info("Some orders have been rejected : {}".format(rejected_orders_to_mail))
                    self.mailer.send_mail('Needle : Rejected Orders', "Rejected Orders : <br>" + rejected_orders_to_mail.to_html())

                all_rejected_orders.to_csv("rejected_trades_today.csv", index=False)
                
            except Exception as ex:
                self.logger.error("Error in exiting from all orders : {}".format(ex))
                self.mailer.send_mail('Needle : Rejected Orders', 'Error in reading rejected orders : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error("Error in fetching all orders : {}".format(ex))
            self.mailer.send_mail('Needle : Rejected Orders', 'Error in fetching all orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    RejectedOrders().execute()