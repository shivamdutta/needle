import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger

class FetchOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files")
            quantity_high = pd.read_csv('quantity_high.csv')
            quantity_low = pd.read_csv('quantity_low.csv')
            self.logger.info("Loaded required files")
            
            try:
                self.logger.debug("Fetching positions of orders placed today")
                positions = self.kite.positions()
                positions_day = positions['day']
                positions_df = pd.DataFrame.from_records(positions_day)
                self.logger.info("Fetched positions of orders placed today")

                try:
                    self.logger.debug("Updating P/L status of executed orders")
                    
                    high_previous_complete_records = quantity_high[(quantity_high['status']=='complete')]
                    low_previous_complete_records = quantity_low[(quantity_low['status']=='complete')]

                    if len(positions_df):
                        for symbol in list(set(positions_df['tradingsymbol'])):

                            high_flag = 1
                            record_to_update = quantity_high[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated')]
                            if len(record_to_update)==0:
                                record_to_update = quantity_low[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated')]
                                high_flag = 0

                            record_to_update['status'] = 'complete'
                            record_to_update['pl_tag'] = round(round(float(positions_df[positions_df['tradingsymbol']==symbol]['pnl']), 2) / (float(record_to_update['squareoff']) * float(record_to_update['quantity'])), 1)
                            record_to_update['profit'] = round(float(positions_df[positions_df['tradingsymbol']==symbol]['pnl']), 2)
                            record_to_update['adhoora_khwab'] = float(record_to_update['actual_khwab']) - float(record_to_update['profit'])
                            record_to_update['flag'] = [1 if abs(float(record_to_update['pl_tag']))==1 else 0]

                            if high_flag==1:

                                previous_complete_records_company = (high_previous_complete_records[high_previous_complete_records['instrument']=='NSE:'+symbol])
                            else:
                                previous_complete_records_company = (low_previous_complete_records[low_previous_complete_records['instrument']=='NSE:'+symbol])

                            latest_complete_record_company = previous_complete_records_company[previous_complete_records_company['trade_number']==previous_complete_records_company['trade_number'].max()]

                            record_to_update['profit_till_now'] = [float(latest_complete_record_company['profit_till_now']) + float(record_to_update['profit']) if len(latest_complete_record_company)==1 else float(record_to_update['profit'])]

                            if high_flag==1:
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'pl_tag'] = float(record_to_update['pl_tag'])
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'profit'] = float(record_to_update['profit'])
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'adhoora_khwab'] = float(record_to_update['adhoora_khwab'])
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'flag'] = int(record_to_update['flag'])
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'profit_till_now'] = float(record_to_update['profit_till_now'])
                                quantity_high.loc[(quantity_high['instrument']=='NSE:'+symbol) & (quantity_high['status']=='to_be_updated'),'status'] = 'complete'
                            else:
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'pl_tag'] = float(record_to_update['pl_tag'])
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'profit'] = float(record_to_update['profit'])
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'adhoora_khwab'] = float(record_to_update['adhoora_khwab'])
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'flag'] = int(record_to_update['flag'])
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'profit_till_now'] = float(record_to_update['profit_till_now'])
                                quantity_low.loc[(quantity_low['instrument']=='NSE:'+symbol) & (quantity_low['status']=='to_be_updated'),'status'] = 'complete'

                    quantity_high = quantity_high.replace({'to_be_updated': 'no_trade'})
                    quantity_low = quantity_low.replace({'to_be_updated': 'no_trade'})

                    quantity_high.to_csv('quantity_high.csv', index=False)
                    quantity_low.to_csv('quantity_low.csv', index=False)

                    self.logger.info("Updated P/L status of executed orders")
                    
                except Exception as ex:
                    self.logger.error('Error in updating P/L status of executed orders : {}'.format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in fetching positions of orders placed today : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files : {}'.format(ex))
            
if __name__ == "__main__":
    
    FetchOrder().execute()