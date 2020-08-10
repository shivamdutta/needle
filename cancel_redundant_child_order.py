import pandas as pd

from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class CancelRedundantChildOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
        
        try:
            self.logger.debug("Loading required files for placing child orders")
            self.trades_today = pd.read_csv('trades_today.csv')
            self.logger.debug("Loaded required files for placing child orders")
        
            try:
                self.logger.debug("Fetching all orders")
                orders = self.kite.orders()
                orders_df = pd.DataFrame(orders)
                self.logger.debug("Fetched all orders")

                try:
                    self.logger.debug("Cancelling redundant child orders")

                    for index, row in self.trades_today.iterrows():
                        
                        try:
                            self.logger.debug('Processing instrument {instrument} for cancelling redundant child orders'.format(instrument=row['instrument']))
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
                            
                            if quantity==0:
                                redundant_orders_instrument = orders_df[(orders_df['tradingsymbol']==row['instrument'][4:]) & (orders_df['status']!='COMPLETE')]
                                
                                #############
                                
                                try:
                                    self.logger.debug("Exiting from redundant child orders for instrument {}".format(row['instrument']))

                                    for index_1, order in redundant_orders_instrument.iterrows():
                                        try:
                                            if (order['status']=='TRIGGER PENDING') | (order['status']=='OPEN'):
                                                self.logger.debug('Exiting order with order_id : {order_id}, variety : {variety} and status : {status}'.format(order_id=order['order_id'], variety=order['variety'], status=order['status']))

                                                if (order['variety']=='bo'):
                                                    var = self.kite.VARIETY_BO
                                                elif (order['variety']=='co'):
                                                    var = self.kite.VARIETY_CO
                                                elif (order['variety']=='regular'):
                                                    var = self.kite.VARIETY_REGULAR
                                                elif (order['variety']=='amo'):
                                                    var = self.kite.VARIETY_AMO

                                                self.kite.exit_order(variety = var, order_id=order['order_id'], parent_order_id=None)
                                                self.logger.info('Exited order with order_id : {order_id}, variety : {variety} and status : {status}'.format(order_id=order['order_id'], variety=order['variety'], status=order['status']))
                                        except Exception as ex:
                                            self.logger.error('Error while exiting order with order_id : {order_id}, variety : {variety} and status : {status} : {ex}'.format(order_id=order['order_id'], variety=order['variety'], status=order['status'], ex=ex))
                                            self.mailer.send_mail('Error while exiting order with order_id : {order_id}, variety : {variety} and status : {status}'.format(order_id=order['order_id'], variety=order['variety'], status=order['status']), str(ex))

                                    self.logger.info("Exited from redundant child orders for instrument {}".format(row['instrument']))

                                except Exception as ex:
                                    self.logger.error("Error in exiting from redundant child orders for instrument {} : {}".format(row['instrument'], ex))
                                    self.mailer.send_mail('Needle : Cancel Redundant Child Order Failure', 'Error in exiting from redundant child orders for instrument {} : {}'.format(row['instrument'], ex))
                                
                                #############
                                
                                        
                            self.logger.debug('Processed instrument {instrument} for cancelling redundant child orders'.format(instrument=row['instrument']))
                        except Exception as ex:
                            self.logger.error('Error while processing instrument {instrument} for cancelling redundant child orders : {ex}'.format(instrument=row['instrument'], ex=ex))
                            self.mailer.send_mail('Error while processing instrument {instrument} for cancelling redundant child orders : {ex}'.format(instrument=row['instrument'], ex=ex))

                    self.logger.debug("Processed all instruments")

                except Exception as ex:
                    self.logger.error("Error while processing all instruments : {}".format(ex))
                    self.mailer.send_mail('Needle : Cancel Redundant Child Order Failure', 'Error while processing all instruments : {}'.format(ex))

            except Exception as ex:
                self.logger.error("Error in fetching all orders : {}".format(ex))
                self.mailer.send_mail('Needle : Cancel Redundant Child Order Failure', 'Error in fetching all orders : {}'.format(ex))
            
        except Exception as ex:
            self.logger.error('Error in loading required files for cancelling redundant child orders : {}'.format(ex))                
            self.mailer.send_mail('Needle : Cancel Redundant Child Order Failure', 'Error in loading required files for cancelling redundant child orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    CancelRedundantChildOrder().execute()