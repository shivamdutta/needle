import pandas as pd
import os.path

from LoggerWrapper import Logger
from Mailer import Mailer

class Quantity:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()

    def decide_quantity(self):
        
        try:
            self.logger.debug('Loading required files for calculating quantity')
            trades_today = pd.read_csv('trades_today.csv')
            budget_df = pd.read_csv('budget.csv')
            if os.path.exists('all_trades.csv'):
                all_trades = pd.read_csv('all_trades.csv')
            else:
                all_trades = pd.DataFrame().reindex_like(trades_today)
            self.logger.info('Loaded required files for calculating quantity')
            
            try:
                self.logger.debug('Calculating quantity')
                
                for index, row in trades_today.iterrows():
                    
                    try:
                        self.logger.debug('Calculating quantity for trading in {} : {}'.format(row['instrument']))
                        
                        last_valid_trade = all_trades[(all_trades['instrument']==row['instrument']) & (all_trades['condition']==row['condition']) & (all_trades['status']==row['complete']) & (all_trades['trade_number']==all_trades['trade_number'].max())]
                        
                        # Calculate trade number
                        if len(last_valid_trade):
                            trade_number = int(last_valid_trade['trade_number']) + 1
                        else:
                            trade_number = 1

                        # Calculate level
                        if len(last_valid_trade):
                            if float(last_valid_trade['pl_tag'])==1.0:
                                level = 1
                            else:
                                level = float(last_valid_trade['level']) + 1
                        else:
                            level = 1
                            
                        # Calculate budget and return
                        if level==1:
                            budget = float(budget_df[budget_df['instrument']==company]['budget'])
                            return_ = float(budget_df[budget_df['instrument']==company]['return'])
                        else:
                            budget = float(last_valid_trade['budget'])
                            return_ = float(last_valid_trade['return'])
                            
                        # Calculate daily khwab
                        if len(last_valid_trade):
                            if int(last_valid_trade['flag'])==1:
                                daily_khwab = round(return_ * budget, 2)
                            else:
                                daily_khwab = 0
                        else:
                            daily_khwab = round(return_ * budget, 2)
                            
                        # Calculate actual khwab
                        if len(last_valid_trade):
                            actual_khwab = max(round(float(last_valid_trade['adhoora_khwab']) + daily_khwab, 2), round(return_ * budget, 2))
                        else:
                            actual_khwab = round(return_ * budget, 2)
                            
                        # Calculate quantity
                        quantity = round(actual_khwab/(return_ * row['open_today']))
                        
                        # Calculate budget required
                        budget_required = round(quantity * row['open_today'], 2)
                        
                        # Calculate price and trigger price
                        if row['condition'] = 'high':
                            price = round(open_today + 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                            trigger_price = round(open_today + 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                        else:
                            price = round(open_today - 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                            trigger_price = round(open_today - 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                            
                        # Calculate square off and stoploss
                        squareoff = round(return_ * price, 1)
                        stoploss = round(return_ * price, 1)
                        
                        # To be updated after placing orders
                        order_id = 'to_be_placed'
                        timestamp = 'to_be_placed'
                        order_type = 'to_be_placed'
                        
                        # To be updated after fetching order status
                        status = 'to_be_placed'
                        profit = 'to_be_placed'
                        pl_tag = 'to_be_placed'
                        flag = 'to_be_placed'
                        adhoora_khwab = 'to_be_placed'
                        
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'trade_number'] = trade_number
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'level'] = level
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'budget'] = budget
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'return_'] = return_
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'daily_khwab'] = daily_khwab
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'actual_khwab'] = actual_khwab
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'quantity'] = quantity
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'budget_required'] = budget_required
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'price'] = price
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'trigger_price'] = trigger_price
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'squareoff'] = squareoff
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'stoploss'] = stoploss
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'order_id'] = order_id
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'timestamp'] = timestamp
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'order_type'] = order_type
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'status'] = status
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'profit'] = profit
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'pl_tag'] = pl_tag
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'flag'] = flag
                        trades_today.loc[trades_today['instrument']==row['instrument'], 'adhoora_khwab'] = adhoora_khwab
                        
                        self.logger.info('Calculated quantity for trading in {} : {}'.format(row['instrument']))
                    except Exception as ex:
                        self.logger.error('Error in calculating quantity for trading in {} : {}'.format(row['instrument'], ex))

                self.logger.info('Calculated quantity')
                
                try:
                    self.logger.debug('Saving files with updated quantity')
                    trades_today.to_csv('trades_today.csv', index=False)
                    self.logger.info('Saved files with updated quantity')
                    self.mailer.send_mail('Needle : Quantities Calculated Successfully', "Trades Today : <br>" + trades_today.to_html())
                except Exception as ex:
                    self.logger.error('Error in saving updated files')
                    self.mailer.send_mail('Needle : Quantities Calculation Failure', 'Error in saving files with updated quantity : {}'.format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in calculating quantity : {}'.format(ex))
                self.mailer.send_mail('Needle : Quantities Calculation Failure', 'Error in calculating quantity : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files for calculating quantity : {}'.format(ex))
            self.mailer.send_mail('Needle : Quantities Calculation Failure', 'Error in loading required files for calculating quantity : {}'.format(ex))
                
if __name__ == "__main__":
    
    Quantity().decide_quantity()        
        