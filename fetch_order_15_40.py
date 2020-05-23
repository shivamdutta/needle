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
                            
                            # Calculate status and profit
                            if len(positions_df[positions_df['tradingsymbol']==row['instrument'][4:]]):
                                status = 'complete'
                                profit = round(float(positions_df[positions_df['tradingsymbol']==row['instrument'][4:]]['pnl']), 2)
                            else:
                                status = 'incomplete'
                                profit = 0
                            
                            # Calculate P/L tag
                            pl_tag = round(round(float(profit), 2) / (float(row['squareoff']) * float(row['quantity'])), 1)
                            
                            # Calculate flag
                            if abs(float(pl_tag))==1:
                                flag = 1
                            else:
                                flag = 0
                            
                            # Calculate adhoora khwab
                            adhoora_khwab = round(float(row['actual_khwab']) - profit, 2)
                                
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'status'] = status
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'profit'] = profit
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'pl_tag'] = pl_tag
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'flag'] = flag
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'adhoora_khwab'] = adhoora_khwab
                            
                            self.logger.info('Updated P/L status of trade in {}'.format(row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error in updating P/L status of trade in {} : {}'.format(row['instrument'], ex))
                            
                            status = 'incomplete'
                            profit = 0
                            pl_tag = 0
                            flag = 0
                            adhoora_khwab = round(float(row['actual_khwab']), 2)
                            
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'status'] = status
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'profit'] = profit
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'pl_tag'] = pl_tag
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'flag'] = flag
                            trades_today.loc[trades_today['instrument']==row['instrument'], 'adhoora_khwab'] = adhoora_khwab
                            
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