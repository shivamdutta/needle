import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceOrderHigh:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
            
    def place_order_high(self, company):

        record_to_trade = self.quantity_high_to_be_placed[self.quantity_high_to_be_placed['instrument']==company].iloc[0]

        tradingsymbol = company[4:]
        transaction_type = self.kite.TRANSACTION_TYPE_BUY
        quantity = int(record_to_trade['quantity'])
        price = float(record_to_trade['price'])
        trigger_price = float(record_to_trade['trigger_price'])
        squareoff = float(record_to_trade['squareoff'])
        stoploss = float(record_to_trade['stoploss'])
        trailing_stoploss = 0
        tag = '{t_no}:{lev}'.format(t_no = int(record_to_trade['trade_number']), lev = int(record_to_trade['level']))

        status_flag = False
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
                        self.logger.warning("LIMIT order placed as SL order placement failed (high) for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                        self.mailer.send_mail('Needle : Place Order Warning', "LIMIT order placed as SL order placement failed (high) for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                    else:
                        raise
                self.logger.info("Order placed ID : {}, instrument : {}".format(order_id, tradingsymbol))
                status_flag = True
                break
            except Exception as ex:
                retry+=1
                self.logger.error("Order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                if retry >= num_retries:
                    self.mailer.send_mail('Needle : Place Order Failure', "Order placement failed for {} with tag {} (high) : {}".format(tradingsymbol, tag, ex))
                
        if status_flag:
            return {'instrument':company, 'order_id':order_id, 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
        else:
            return {'instrument':company, 'order_id':'order_failed', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
            
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for high trades")
            self.quantity_high_to_be_placed = pd.read_csv('quantity_high_to_be_placed.csv')
            self.logger.info("Loaded required files for high trades")
            
            try:
                self.logger.debug("Placing orders (high)")
                companies_to_trade_high = list(set(self.quantity_high_to_be_placed['instrument']))
                n_companies_to_trade_high = len(companies_to_trade_high)
                if n_companies_to_trade_high:
                    multithreading = True
                    if multithreading:
                        pool = ThreadPool(n_companies_to_trade_high)
                        orders = pool.map(self.place_order_high, companies_to_trade_high)
                    else:
                        orders = []
                        for c in companies_to_trade_high:
                            order = self.place_order_high(c)
                            orders.append(order)
                    orders_df = pd.DataFrame(orders)
                self.logger.info("Placed orders (high)")
                                  
                try:
                    self.logger.debug("Saving updated files for high trades")
                    
                    self.quantity_high = pd.read_csv('quantity_high.csv')
                    
                    for company in companies_to_trade_high:
                        order_status = orders_df[orders_df['instrument']==company].iloc[0]
                        if order_status['order_id']=='order_failed':
                            
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'pl_tag'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'profit'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'flag'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'profit_till_now'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'status'] = 'order_failed'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'pl_tag'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'profit'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'flag'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'profit_till_now'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'status'] = 'order_failed'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                        else:
                            
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'pl_tag'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'profit'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'flag'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'profit_till_now'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'status'] = 'to_be_updated'
                            self.quantity_high.loc[(self.quantity_high['instrument']==company) & (self.quantity_high['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'timestamp'] = order_status['timestamp']
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'pl_tag'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'profit'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'adhoora_khwab'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'flag'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'profit_till_now'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'status'] = 'to_be_updated'
                            self.quantity_high_to_be_placed.loc[(self.quantity_high_to_be_placed['instrument']==company) & (self.quantity_high_to_be_placed['order_id']=='to_be_placed'), 'order_id'] = order_status['order_id']
                            
                    self.quantity_high.to_csv('quantity_high.csv', index=False)
                    self.quantity_high_to_be_placed.to_csv('quantity_high_to_be_placed.csv', index=False)
                    self.logger.info("Saved updated files for high trades")
                    self.mailer.send_mail('Needle : Orders (High) Placed Successfully', "Quantity Table (High) : <br>" + self.quantity_high_to_be_placed.to_html())
                    
                except Exception as ex:
                    self.logger.error("Error in saving updated files for high trades : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Order Failure', "Error in saving updated files for high trades : {}".format(ex))
                    
            except Exception as ex:
                self.logger.error('Error in placing orders (high) : {}'.format(ex))
                self.mailer.send_mail('Needle : Place Order Failure', "Error in placing orders (high) : {}".format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files for high trades : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Order Failure', 'Error in loading required files for high trades : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceOrderHigh().execute()