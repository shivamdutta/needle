import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceChildOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def place_order_child(self, tag, order_type, instrument, transaction_type, quantity, price, trigger_price=None):
        
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
                if order_type=='SL':
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
                else:
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
                    self.logger.debug("Placing child orders")

                    for index, row in self.trades_today.iterrows():
                        
                        try:
                            self.logger.debug('Processing instrument {instrument} for placing child orders'.format(instrument=row['instrument']))
                            orders_instrument = orders_df[orders_df['tradingsymbol']==row['instrument'][4:]]
                            if len(orders_instrument.transaction_type.unique().tolist())==1:
                                for index_1, row_1 in orders_instrument.iterrows():
                                    if row_1['product']!='BO':
                                        if row_1['status']=='COMPLETE':
                                            parent_order_id = row_1['order_id']
                                            parent_quantity = row_1['quantity']
                                            parent_order_transaction_type = row_1['transaction_type']

                                            price = float(row['price'])
                                            squareoff = float(row['squareoff'])
                                            stoploss = float(row['stoploss'])

                                            if parent_order_transaction_type=='BUY':

                                                transaction_type='sell'

                                                target_price = price + squareoff

                                                stoploss_price = price - stoploss
                                                stoploss_trigger_price = stoploss_price + 1

                                            else:

                                                transaction_type='buy'

                                                target_price = price - squareoff

                                                stoploss_price = price + stoploss
                                                stoploss_trigger_price = stoploss_price - 1

                                            # Place target order
                                            target_order = self.place_order_child(tag=parent_order_id, order_type='LIMIT', instrument=row['instrument'], transaction_type=transaction_type, quantity=parent_quantity, price=target_price)
                                            self.logger.info('Placed target order for {instrument} and parent order id : {parent_order_id} : {target_order}'.format(instrument=row['instrument'], parent_order_id=parent_order_id, target_order=target_order))

                                            # Place stoploss order
                                            stoploss_order = self.place_order_child(tag=parent_order_id, order_type='SL', instrument=row['instrument'], transaction_type=transaction_type, quantity=parent_quantity, price=stoploss_price, trigger_price=stoploss_trigger_price)
                                            self.logger.info('Placed stoploss order for {instrument} and parent order id : {parent_order_id} : {stoploss_order}'.format(instrument=row['instrument'], parent_order_id=parent_order_id, stoploss_order=stoploss_order))
                                        
                            self.logger.info('Processed instrument {instrument} for placing child orders'.format(instrument=row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error while processing instrument {instrument} for placing child orders : {ex}'.format(instrument=row['instrument'], ex=ex))
                            self.mailer.send_mail('Error while processing instrument {instrument} for placing child orders : {ex}'.format(instrument=row['instrument'], ex=ex))

                    self.logger.info("Processed all instruments")
                    self.mailer.send_mail('Needle : Placed Child Orders Successfully', "All orders list : <br>" + pd.DataFrame(self.kite.orders()).to_html())

                except Exception as ex:
                    self.logger.error("Error while processing all instruments : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Child Order Failure', 'Error while processing all instruments : {}'.format(ex))

            except Exception as ex:
                self.logger.error("Error in fetching all orders : {}".format(ex))
                self.mailer.send_mail('Needle : Place Child Order Failure', 'Error in fetching all orders : {}'.format(ex))
            
        except Exception as ex:
            self.logger.error('Error in loading required files for placing child orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Child Order Failure', 'Error in loading required files for placing child orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceChildOrder().execute()