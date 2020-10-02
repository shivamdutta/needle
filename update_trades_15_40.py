import pandas as pd
import os.path

from LoggerWrapper import Logger
from Mailer import Mailer

class UpdateTrades:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for updating trades")
            trades_today = pd.read_csv('trades_today.csv')
            budget_df = pd.read_csv('budget.csv')
            if os.path.exists('all_trades.csv'):
                all_trades = pd.read_csv('all_trades.csv')
            else:
                all_trades = pd.DataFrame(columns=trades_today.columns)
            self.logger.info("Loaded required files for updating trades")
            
            try:
                self.logger.debug("Updating trades in main table")
                all_trades = all_trades.append(trades_today, ignore_index=True)
                self.logger.info("Updated trades in main table")

                try:
                    self.logger.debug("Saving main table")
                    all_trades.to_csv('all_trades.csv', index=False)
                    self.logger.info("Saved main table")
                    self.mailer.send_mail('Needle : Updated Trades Successfully (Net PnL : {})'.format(round(all_trades.profit.sum(), 2)), "All Trades : <br>" + all_trades.drop_duplicates(subset=['instrument', 'condition'], keep='last').sort_values('adhoora_khwab', ascending=False).to_html())
                    
                except Exception as ex:
                    self.logger.error('Error in saving main table : {}'.format(ex))
                    self.mailer.send_mail('Needle : Update Trades Failure', 'Error in saving main table : {}'.format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in updating trades in main table : {}'.format(ex))
                self.mailer.send_mail('Needle : Update Trades Failure', 'Error in updating trades in main table : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files for updating trades : {}'.format(ex))
            self.mailer.send_mail('Needle : Update Trades Failure', 'Error in loading required files for updating trades : {}'.format(ex))
            
if __name__ == "__main__":
    
    UpdateTrades().execute()