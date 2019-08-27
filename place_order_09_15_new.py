import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import time

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def place_order(self, instrument):

        record_to_trade = self.trades_today[self.trades_today['instrument']==instrument].iloc[0]

        tradingsymbol = instrument[4:]
        
        if record_to_trade['transaction_type']=='buy':
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
            
        quantity = int(record_to_trade['quantity'])
        price = float(record_to_trade['price'])
        trigger_price = float(record_to_trade['trigger_price'])
        squareoff = float(record_to_trade['squareoff'])
        stoploss = float(record_to_trade['stoploss'])
        trailing_stoploss = 0
        tag = '{t_no}:{lev}'.format(t_no = int(record_to_trade['trade_number']), lev = int(record_to_trade['level']))

        status_flag = False
        limit_order_flag = False
        retry = 0
        num_retries = 3
        while status_flag is not True and retry < num_retries:
            if retry:
                self.logger.info("Retrying...")
            try:
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
                except Exception as ex:
                    if str(ex)[0:13]=='Trigger price':
                        order_id = self.kite.place_order(variety=self.kite.VARIETY_BO,
                                            exchange=self.kite.EXCHANGE_NSE,
                                            tradingsymbol=tradingsymbol,
                                            transaction_type=transaction_type,
                                            quantity=quantity,
                                            order_type=self.kite.ORDER_TYPE_LIMIT,
                                            product=self.kite.PRODUCT_MIS,
                                            price=price,
                                            validity=self.kite.VALIDITY_DAY,
                                            disclosed_quantity=None,
                                            trigger_price=None,
                                            squareoff=squareoff,
                                            stoploss=stoploss,
                                            trailing_stoploss=trailing_stoploss,
                                            tag=tag)
                        limit_order_flag = True
                        self.logger.warning("LIMIT order placed as SL order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                    else:
                        raise
                self.logger.info("Order placed ID : {}, instrument : {}".format(order_id, tradingsymbol))
                status_flag = True
                break
            except Exception as ex:
                retry+=1
                self.logger.error("Order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                if retry >= num_retries:
                    self.mailer.send_mail('Needle : Place Order Failure', "Order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                
        if status_flag:
            if limit_order_flag:
                return {'instrument':instrument, 'order_id':order_id, 'order_type':'LIMIT', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
            else:
                return {'instrument':instrument, 'order_id':order_id, 'order_type':'SL', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
        else:
            return {'instrument':instrument, 'order_id':'order_failed', 'order_type':'order_failed', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
            
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for placing orders")
            self.trades_today = pd.read_csv('trades_today.csv')
            self.logger.info("Loaded required files for placing orders")
            
            try:
                self.logger.debug("Placing orders")
                time.sleep(58)
                instruments_to_trade = list(set(self.trades_today['instrument']))
                n_instruments_to_trade = len(instruments_to_trade)
                if n_instruments_to_trade:
                    multithreading = True
                    if multithreading:
                        pool = ThreadPool(n_instruments_to_trade)
                        orders = pool.map(self.place_order, instruments_to_trade)
                    else:
                        orders = []
                        for c in instruments_to_trade:
                            order = self.place_order(c)
                            orders.append(order)
                    orders_df = pd.DataFrame(orders)
                self.logger.info("Placed orders")
                                  
                try:
                    self.logger.debug("Saving updated files after placing orders")
                    
                    for instrument in instruments_to_trade:
                        
                        order_status = orders_df[orders_df['instrument']==instrument].iloc[0]
                        
                        if order_status['order_id']=='order_failed':
                            
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'order_type'] = order_status['order_type']
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'status'] = 'order_failed'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'profit'] = 'order_failed'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'pl_tag'] = 'order_failed'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'flag'] = 'order_failed'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'order_failed'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'profit_till_now'] = 'order_failed'
                            
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                        else:
                            
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'order_type'] = order_status['order_type']
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'status'] = 'to_be_updated'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'profit'] = 'to_be_updated'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'pl_tag'] = 'to_be_updated'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'flag'] = 'to_be_updated'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'to_be_updated'
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'profit_till_now'] = 'to_be_updated'
                            
                            self.trades_today.loc[(self.trades_today['instrument']==instrument) & (self.trades_today['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                    self.trades_today.to_csv('trades_today.csv', index=False)
                    self.logger.info("Saved updated files after placing orders")
                    self.mailer.send_mail('Needle : Orders Placed Successfully', "Trades Today : <br>" + self.trades_today.to_html())
                    
                except Exception as ex:
                    self.logger.error("Error in saving updated files after placing orders : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Order Failure', "Error in saving updated files after placing orders : {}".format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in placing orders : {}'.format(ex))
                self.mailer.send_mail('Needle : Place Order Failure', "Error in placing orders : {}".format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files for placing orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Order Failure', 'Error in loading required files for placing orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceOrder().execute()