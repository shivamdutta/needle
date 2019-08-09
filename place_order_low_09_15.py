import pandas as pd
import time

from LogIn import LogIn
from LoggerWrapper import Logger

class PlaceOrderLow:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        
    def place_order_low(company):

        record_to_trade = self.quantity_low_to_be_placed[self.quantity_low_to_be_placed['instrument']==company]

        tradingsymbol = company[4:]
        transaction_type = self.kite.TRANSACTION_TYPE_SELL
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
            if retry:
                self.logger.info("Retrying...")                        
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

                self.logger.info("Order placed ID : {}, instrument : {}".format(order_id, tradingsymbol))
                status_flag = True
                break
            except Exception as ex:
                retry+=1
                self.logger.error("Order placement failed: {}".format(ex))

        if status_flag:
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'order_id'] = order_id
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'timestamp'] = int(time.time())
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'pl_tag'] = 'to_be_updated'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'profit'] = 'to_be_updated'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'to_be_updated'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'flag'] = 'to_be_updated'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'profit_till_now'] = 'to_be_updated'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'status'] = 'to_be_updated'
        else:
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'order_id'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'timestamp'] = int(time.time())
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'pl_tag'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'profit'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'flag'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'profit_till_now'] = 'order_failed'
            self.quantity_low.loc[(self.quantity_low['instrument']==company) & (self.quantity_low['order_id']=='to_be_placed'), 'status'] = 'order_failed'
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for low trades")
            self.quantity_low = pd.read_csv('quantity_low.csv')
            self.quantity_low_to_be_placed = self.quantity_low[self.quantity_low['order_id']=='to_be_placed']
            self.logger.info("Loaded required files for low trades")
            
            try:
                self.logger.debug("Placing orders (low)")
                pool = ThreadPool(len(list(set(self.quantity_low_to_be_placed['instrument']))))
                pool.map(self.place_order_low, list(set(self.quantity_low_to_be_placed['instrument'])))
                self.logger.info("Placed orders (low)")
                
                try:
                    self.logger.debug("Saving updated files for low trades")
                    self.quantity_low.to_csv('quantity_low.csv', index=False)
                    self.logger.info("Saved updated files for low trades")
                except Exception as ex:
                    self.logger.error("Error in saving updated files for low trades: {}".format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in placing orders (low) : {}'.format(ex))

        except Exception as ex:
            self.logger.error('Error in loading required files for low trades : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceOrderLow().execute()