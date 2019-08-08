import pandas as pd
import time

from LogIn import LogIn
from LoggerWrapper import Logger

class PlaceOrderHigh:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files")
            quantity_high = pd.read_csv('quantity_high.csv')
            self.logger.info("Loaded required files")
            
            try:
                self.logger.debug("Placing orders (high)")
                
                # Place orders (high) : Start

                high_records_traded_today = pd.DataFrame()
                quantity_high_to_be_placed = quantity_high[quantity_high['order_id']=='to_be_placed']

                for company in list(set(quantity_high_to_be_placed['instrument'])):

                    record_to_trade = quantity_high_to_be_placed[quantity_high_to_be_placed['instrument']==company]

                    tradingsymbol = company[4:]
                    transaction_type = self.kite.TRANSACTION_TYPE_BUY
                    quantity = int(record_to_trade['quantity'])
                    price = float(record_to_trade['price'])
                    trigger_price = float(record_to_trade['trigger_price'])
                    squareoff = float(record_to_trade['squareoff'])
                    stoploss = float(record_to_trade['stoploss'])
                    trailing_stoploss = 0
                    tag = '{t_no}:{lev}'.format(t_no = int(record_to_trade['trade_number']), lev = float(record_to_trade['level']))

                    status_flag = False
                    retry = 0
                    num_retries = 3
                    while status_flag is not True and retry < num_retries:
                        try:
                            order_id = self.kite.place_order(variety=self.kite.VARIETY_BO,
                                                        exchange=self.kite.EXCHANGE_NSE,
                                                        tradingsymbol=tradingsymbol,
                                                        transaction_type=transaction_type,
                                                        quantity=quantity,
                                                        order_type=self.kite.ORDER_TYPE_SL,
                                                        product=self.kite.PRODUCT_MIS,
                                                        price=price,
                                                        validity=self.kite.VALIDITY_DAY,
                                                        disclosed_quantity=None,
                                                        trigger_price=trigger_price,
                                                        squareoff=squareoff,
                                                        stoploss=stoploss,
                                                        trailing_stoploss=trailing_stoploss,
                                                        tag=tag)

                            record_to_trade['order_id'] = order_id
                            record_to_trade['timestamp'] = time.time()

                            record_to_trade['pl_tag'] = 'to_be_updated'
                            record_to_trade['profit'] = 'to_be_updated'
                            record_to_trade['adhoora_khwab'] = 'to_be_updated'
                            record_to_trade['flag'] = 'to_be_updated'
                            record_to_trade['profit_till_now'] = 'to_be_updated'
                            record_to_trade['status'] = 'to_be_updated'

                            self.logger.info("Order placed. ID is: {}".format(order_id))
                            high_records_traded_today = high_records_traded_today.append(record_to_trade, ignore_index=True)

                            status_flag = True
                            break
                        except Exception as ex:
                            retry+=1
                            self.logger.error("Order placement failed: {}".format(ex))
                            self.logger.info("Retrying........")

                high_records_placed_before = quantity_high[quantity_high['order_id']!='to_be_placed']
                quantity_high = high_records_placed_before.append(high_records_traded_today, ignore_index=True)

                quantity_high.to_csv('quantity_high.csv', index=False)

                # Place orders (high) : End
                
                self.logger.info("Placed orders (high)")
                
            except Exception as ex:
                self.logger.error('Error in placing orders (high) : {}'.format(ex))

        except Exception as ex:
            self.logger.error('Error in loading required files : {}'.format(ex))                
                
if __name__ == "__main__":
    
    PlaceOrderHigh().execute()