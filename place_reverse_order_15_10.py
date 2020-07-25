import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceReverseOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def place_reverse_order(self, instrument, transaction_type, quantity, tag):
        
        tradingsymbol = instrument[4:]
        
        if transaction_type=='buy':
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
            
        status_flag = False
        retry = 0
        num_retries = 3
        while status_flag is not True and retry < num_retries:
            if retry:
                self.logger.info("Retrying...")
            try:
                order_id = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                    exchange=self.kite.EXCHANGE_NSE,
                                    tradingsymbol=tradingsymbol,
                                    transaction_type=transaction_type,
                                    quantity=quantity,
                                    order_type=self.kite.ORDER_TYPE_MARKET,
                                    product=self.kite.PRODUCT_MIS,
                                    price=None,
                                    validity=self.kite.VALIDITY_DAY,
                                    disclosed_quantity=None,
                                    trigger_price=None,
                                    squareoff=None,
                                    stoploss=None,
                                    trailing_stoploss=None,
                                    tag=tag)
                        
                self.logger.info("Order placed ID : {}, instrument : {}".format(order_id, tradingsymbol))
                status_flag = True
                break
            except Exception as ex:
                retry+=1
                self.logger.error("Order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                if retry >= num_retries:
                    self.mailer.send_mail('Needle : Place Order Failure', "Order placement failed for {} with tag {} : {}".format(tradingsymbol, tag, ex))
                
        if status_flag:
            return {'instrument':instrument, 'order_id':order_id, 'order_type':'MARKET', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
        else:
            return {'instrument':instrument, 'order_id':-1, 'order_type':'NONE', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for placing reverse orders")
            self.trades_today = pd.read_csv('trades_today.csv')
            self.logger.info("Loaded required files for placing reverse orders")
        
            try:
                self.logger.debug("Fetching all orders")
                orders = self.kite.orders()
                orders_df = pd.DataFrame(orders)
                self.logger.debug("Fetched all orders")

                try:
                    self.logger.debug("Placing reverse orders")

                    for index, row in self.trades_today.iterrows():
                        
                        try:
                            self.logger.debug('Processing instrument {instrument} for placing reverse orders'.format(instrument=row['instrument']))
                            orders_instrument = orders_df[(orders_df['tradingsymbol']==row['instrument'][4:]) & (orders_df['status']=='COMPLETE')]
                            
                            quantities_bought = 0
                            quantities_sold = 0
                            for trans_type in orders_instrument.transaction_type.unique().tolist():
                                trans_quantity_sum = orders_instrument[orders_instrument['transaction_type']==trans_type].quantity.sum()
                                if trans_type=='BUY':
                                    quantities_bought = quantities_bought + trans_quantity_sum
                                else:
                                    quantities_sold = quantities_sold + trans_quantity_sum

                            if quantities_bought>quantities_sold:
                                reverse_order = self.place_reverse_order(instrument=row['instrument'], transaction_type='sell', quantity=int(quantities_bought-quantities_sold), tag='reverse')
                            elif quantities_bought<quantities_sold:
                                reverse_order = self.place_reverse_order(instrument=row['instrument'], transaction_type='sell', quantity=int(quantities_sold-quantities_bought), tag='reverse')
                            
                            self.logger.info('Placed reverse order for {instrument} : {reverse_order}'.format(instrument=row['instrument'], reverse_order=reverse_order))
                                        
                            self.logger.info('Processed instrument {instrument} for placing reverse order'.format(instrument=row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error while processing instrument {instrument} for placing reverse order : {ex}'.format(instrument=row['instrument'], ex=ex))
                            self.mailer.send_mail('Error while processing instrument {instrument} for placing reverse order : {ex}'.format(instrument=row['instrument'], ex=ex))

                    self.logger.info("Processed all instruments")
                    self.mailer.send_mail('Needle : Placed Reverse Orders Successfully', "All orders list : <br>" + pd.DataFrame(self.kite.orders()).to_html())

                except Exception as ex:
                    self.logger.error("Error while processing all instruments : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error while processing all instruments : {}'.format(ex))

            except Exception as ex:
                self.logger.error("Error in fetching all orders : {}".format(ex))
                self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error in fetching all orders : {}'.format(ex))
            
        except Exception as ex:
            self.logger.error('Error in loading required files for placing reverse orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error in loading required files for placing reverse orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceReverseOrder().execute()