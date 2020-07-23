import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceChildOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def place_order(self, instrument, transaction_type, quantity, price, trigger_price, tag):
        
        tradingsymbol = instrument[4:]
        
        if transaction_type=='buy':
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
            
        status_flag = False
        limit_order_flag = False
        retry = 0
        num_retries = 3
        while status_flag is not True and retry < num_retries:
            if retry:
                self.logger.info("Retrying...")
            try:
                try:
                    order_id = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
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
                                            squareoff=None,
                                            stoploss=None,
                                            trailing_stoploss=None,
                                            tag=tag)
                except Exception as ex:
                    if str(ex)[0:13]=='Trigger price':
                        order_id = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
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
                                            squareoff=None,
                                            stoploss=None,
                                            trailing_stoploss=None,
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
            return {'instrument':instrument, 'order_id':-1, 'order_type':'NONE', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330)}
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for placing child orders")
            self.trades_today = pd.read_csv('trades_today.csv')
            self.logger.info("Loaded required files for placing child orders")
        
            try:
                self.logger.debug("Fetching all orders")
                orders = self.kite.orders()
                orders_df = pd.DataFrame(orders)
                self.logger.debug("Fetched all orders")

                try:
                    self.logger.debug("Exiting from all orders")

                    for tradingsymbol in orders_df.tradingsymbol.unique().tolist():
                        
                        instrument = 'NSE:' + tradingsymbol
                        try:
                            self.logger.debug('Processing instrument {instrument} for placing child orders'.format(instrument=instrument))
                            orders = orders_df[orders_df['tradingsymbol']==tradingsymbol]
                            if len(orders)==1:
                                if orders.product.iloc[0]!='BO':
                                    if orders.status.iloc[0]=='COMPLETE':
                                        
                                        parent_order_id = orders.order_id.iloc[0]
                                        parent_quantity = orders.quantity.iloc[0]
                                        parent_order_transaction_type = orders.transaction_type.iloc[0]
                                        if parent_order_transaction_type=='BUY':
                                            transaction_type='sell'
                                        else:
                                            transaction_type='buy'
                                        
                                        parent_order_system_details = self.trades_today[self.trades_today['instrument']==instrument].iloc[0]
                                        
                                        squareoff = float(parent_order_system_details['squareoff'])
                                        stoploss = float(parent_order_system_details['stoploss'])
                                        
                                        if transaction_type=='buy':
                                            trigger_delta = 1
                                        else:
                                            trigger_delta = -1
                                            
                                        # Place target order
                                        target_order = place_order(instrument=instrument, transaction_type=transaction_type, quantity=parent_quantity, price=squareoff, trigger_price=squareoff+trigger_delta, tag=parent_order_id)
                                        self.logger.info('Placed target order for {instrument} and parent order id : {parent_order_id} : {target_order}'.format(instrument=instrument, parent_order_id=parent_order_id, target_order=target_order))
                                        
                                        # Place stoploss order
                                        stoploss_order = place_order(instrument=instrument, transaction_type=transaction_type, quantity=parent_quantity, price=stoploss, trigger_price=stoploss+trigger_delta, tag=parent_order_id)
                                        self.logger.info('Placed stoploss order for {instrument} and parent order id : {parent_order_id} : {stoploss_order}'.format(instrument=instrument, parent_order_id=parent_order_id, stoploss_order=stoploss_order))
                                        
                            self.logger.info('Processed instrument {instrument} for placing child orders'.format(instrument=instrument))
                        except Exception as ex:
                            self.logger.error('Error while processing instrument {instrument} for placing child orders : {ex}'.format(instrument=instrument, ex=ex))
                            self.mailer.send_mail('Error while processing instrument {instrument} for placing child orders : {ex}'.format(instrument=instrument, ex=ex))

                    self.logger.info("Processed all instruments")
                    self.mailer.send_mail('Needle : Placed Child Orders Successfully', "All orders list : <br>" + pd.DataFrame(self.kite.orders()).to_html())

                except Exception as ex:
                    self.logger.error("Error while processing all instruments : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Child Order Failure', 'Error in placing child orders : {}'.format(ex))

            except Exception as ex:
                self.logger.error("Error in fetching all orders : {}".format(ex))
                self.mailer.send_mail('Needle : Place Child Order Failure', 'Error in placing child orders : {}'.format(ex))
            
        except Exception as ex:
            self.logger.error('Error in loading required files for placing child orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Child Order Failure', 'Error in loading required files for placing child orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceChildOrder().execute()