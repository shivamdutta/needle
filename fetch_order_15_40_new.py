import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class FetchOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for updating P/L status")
            trades_today = pd.read_csv('trades_today.csv')
            self.logger.info("Loaded required files for updating P/L status")
            
            try:
                self.logger.debug("Fetching positions of orders placed today")
                positions = self.kite.positions()
                positions_day = positions['day']
                positions_df = pd.DataFrame.from_records(positions_day)
                self.logger.info("Fetched positions of orders placed today")

                try:
                    self.logger.debug("Updating P/L status of executed orders")

                    if len(positions_df):
                        for symbol in list(set(positions_df['tradingsymbol'])):

                            record_to_update = trades_today[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated')]
                            
                            if len(record_to_update):
                                
                                profit = round(float(positions_df[positions_df['tradingsymbol']==symbol]['pnl']), 2)
                                pl_tag = round(round(float(profit), 2) / (float(record_to_update['squareoff']) * float(record_to_update['quantity'])), 1)
                                if abs(float(pl_tag))==1:
                                    flag = 1
                                else:
                                    flag = 0
                                adhoora_khwab = round(float(record_to_update['actual_khwab']) - profit, 2)
                                
                                trades_today.loc[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated'),'profit'] = profit
                                trades_today.loc[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated'),'pl_tag'] = pl_tag
                                trades_today.loc[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated'),'flag'] = flag
                                trades_today.loc[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated'),'adhoora_khwab'] = adhoora_khwab
                                
                                trades_today.loc[(trades_today['instrument']=='NSE:'+symbol) & (trades_today['status']=='to_be_updated'),'status'] = 'complete'
                                
                                    
                    trades_today = trades_today.replace({'to_be_updated': 'no_trade'})
                    trades_today.to_csv('trades_today.csv', index=False)
                    
                    self.logger.info("Updated P/L status of executed orders")
                    self.mailer.send_mail('Needle : Updated P/L Status Successfully', "Trades Today : <br>" + trades_today.to_html())
                    
                except Exception as ex:
                    self.logger.error('Error in updating P/L status of executed orders : {}'.format(ex))
                    self.mailer.send_mail('Needle : P/L Status Update Failure', 'Error in updating P/L status of executed orders : {}'.format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in fetching positions of orders placed today : {}'.format(ex))
                self.mailer.send_mail('Needle : P/L Status Update Failure', 'Error in fetching positions of orders placed today : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files for updating P/L status : {}'.format(ex))
            self.mailer.send_mail('Needle : P/L Status Update Failure', 'Error in loading required files for updating P/L status : {}'.format(ex))
            
if __name__ == "__main__":
    
    FetchOrder().execute()