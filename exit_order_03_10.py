from LogIn import LogIn
from LoggerWrapper import Logger
from Mailer import Mailer

class ExitOrder:
    
    def __init__(self):
        self.kite = LogIn().return_kite_obj()
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def execute(self):
        
        try:
            self.logger.debug("Exiting from all orders")

            orders = self.kite.orders()

            for order in orders:

                order_id = order['order_id']
                self.logger.info('Exiting order with order_id : {order_id} and status : {status}'.format(order_id=order_id, status=order['status']))
                self.kite.exit_order(variety = self.kite.VARIETY_BO, order_id=order_id, parent_order_id=None)

            self.logger.info("Successfully exited from all orders")
            self.mailer.send_mail('Needle : Exited Orders Successfully', 'Exited from all orders (if any)')
        except Exception as ex:
            self.logger.error("Error in exiting from all orders : {}".format(ex))
            self.mailer.send_mail('Needle : Exit Order Failure', 'Error in exiting from all orders : {}'.format(ex))
            
if __name__ == "__main__":
    
    ExitOrder().execute()