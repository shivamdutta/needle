import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class PlaceReverseOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def place_reverse_order(self, instrument):
        
        record_to_trade = self.reverse_orders_to_place[self.reverse_orders_to_place['instrument']==instrument].iloc[0]
        
        tradingsymbol = instrument[4:]
        
        if record_to_trade['transaction_type']=='buy':
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
            
        quantity = int(record_to_trade['quantity'])
        tag = 'reverse'
        
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
            return {'instrument':instrument, 'order_id':order_id, 'order_type':'MARKET', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330), 'transaction_type':transaction_type, 'quantity':quantity, 'tag':tag}
        else:
            return {'instrument':instrument, 'order_id':-1, 'order_type':'NONE', 'timestamp':pd.Timestamp.now()+pd.DateOffset(minutes=330), 'transaction_type':transaction_type, 'quantity':quantity, 'tag':tag}
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for placing reverse orders")
            self.trades_today = pd.read_csv('trades_today.csv')
            self.logger.info("Loaded required files for placing reverse orders")
        
            try:
                self.logger.debug("Fetching orders placed today and their positions")
                
                orders_df = pd.DataFrame(self.kite.orders())
                orders_df = orders_df[orders_df['product']=='MIS'][['tradingsymbol', 'transaction_type', 'status', 'order_timestamp',  'order_type', 'tag', 'price', 'trigger_price', 'average_price', 'quantity']]
                
                positions_df = pd.DataFrame.from_records(self.kite.positions()['day'])
                positions_df = positions_df[positions_df['product']=='MIS'][['tradingsymbol', 'pnl', 'quantity', 'buy_price', 'sell_price', 'last_price']]
                
                self.mailer.send_mail("Needle : Positions & Orders Update (Net PnL : {})".format(round(positions_df.pnl.sum(), 2)), "Positions : <br>" + positions_df.to_html() + " <br> Orders : <br>" + orders_df.to_html())
                
                self.logger.info("Fetched orders placed today and their positions")

                try:
                    self.logger.debug("Placing reverse orders")

                    self.reverse_orders_to_place = pd.DataFrame()
                    
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

                            quantity = int(abs(quantities_bought-quantities_sold))

                            if quantity:
                                if quantities_bought>quantities_sold:
                                    transaction_type = 'sell'
                                elif quantities_bought<quantities_sold:
                                    transaction_type = 'buy'
                                reverse_order_to_place = pd.DataFrame(data={'instrument':[row['instrument']], 'transaction_type':[transaction_type], 'quantity':[quantity]})
                                self.reverse_orders_to_place = self.reverse_orders_to_place.append(reverse_order_to_place, ignore_index=True)
                            
                            self.logger.info('Processed instrument {instrument} for placing reverse order'.format(instrument=row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error while processing instrument {instrument} for placing reverse order : {ex}'.format(instrument=row['instrument'], ex=ex))
                            self.mailer.send_mail('Error while processing instrument {instrument} for placing reverse order : {ex}'.format(instrument=row['instrument'], ex=ex))

                    
                    n_instruments_to_place_reverse_orders = len(self.reverse_orders_to_place)
                    if n_instruments_to_place_reverse_orders:
                        pool = ThreadPool(n_instruments_to_place_reverse_orders)
                        reverse_orders = pool.map(self.place_reverse_order, list(set(self.reverse_orders_to_place['instrument'])))
                        reverse_orders_df = pd.DataFrame(reverse_orders)
                        self.logger.info("Placed reverse orders successfully : {}".format(reverse_orders))
                        self.mailer.send_mail('Needle : Placed Reverse Orders Successfully', "Reverse orders : <br>" + reverse_orders_df.to_html())
                        
                except Exception as ex:
                    self.logger.error("Error while placing reverse orders : {}".format(ex))
                    self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error while placing reverse orders : {}'.format(ex))

            except Exception as ex:
                self.logger.error("Error in fetching orders placed today and their positions : {}".format(ex))
                self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error in fetching orders placed today and their positions : {}'.format(ex))
            
        except Exception as ex:
            self.logger.error('Error in loading required files for placing reverse orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Place Reverse Order Failure', 'Error in loading required files for placing reverse orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    PlaceReverseOrder().execute()