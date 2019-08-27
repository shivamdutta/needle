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
                    
                    for index, row in trades_today.iterrows():
                        
                        try:
                            self.logger.debug('Updating P/L status of trade in {}'.format(row['instrument']))
                            
                            if len(positions_df[positions_df['tradingsymbol']==row['instrument'][4:]]):
                                profit = round(float(positions_df[positions_df['tradingsymbol']==row['instrument'][4:]]['pnl']), 2)
                                pl_tag = round(round(float(profit), 2) / (float(row['squareoff']) * float(row['quantity'])), 1)
                                if abs(float(pl_tag))==1:
                                    flag = 1
                                else:
                                    flag = 0
                                adhoora_khwab = round(float(row['actual_khwab']) - profit, 2)
                            
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'profit'] = profit
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'pl_tag'] = pl_tag
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'flag'] = flag
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'adhoora_khwab'] = adhoora_khwab
                                
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'status'] = 'complete'
                            
                            else:
                                
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'profit'] = 'no_trade'
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'pl_tag'] = 'no_trade'
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'flag'] = 'no_trade'
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'adhoora_khwab'] = 'no_trade'
                                
                                trades_today.loc[(trades_today['instrument']==row['instrument']) & (trades_today['status']=='to_be_updated'),'status'] = 'no_trade'
                                
                            self.logger.info('Updated P/L status of trade in {}'.format(row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error in updating P/L status of trade in {} : {}'.format(row['instrument'], ex))
                            
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