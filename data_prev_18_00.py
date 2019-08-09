import pandas as pd

from LoggerWrapper import Logger
from Mailer import Mailer
from Ohlc import Ohlc

class SaveCurrentOhlc:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def save_ohlc(self):
        
        try:
            self.logger.debug("Fetching current ohlc")
            self.current_ohlc_data = Ohlc().fetch_current_ohlc()
            ohlc_final_df = self.current_ohlc_data.rename(columns={'index': 'instrument', 'last_price':'last_price_prev', 'open':'open_prev', 'high':'high_prev', 'low':'low_prev', 'close':'close_prev'})
            ohlc_final_df.to_csv('ohlc_previous.csv', index=False)
            self.logger.info("Fetched and saved current ohlc")
            self.mailer.send_mail('Needle : Current OHLC Saved Successfully', ohlc_final_df.to_html())
        except Exception as ex:
            self.logger.error("Error while fetching and saving current ohlc : {}".format(ex))
            self.mailer.send_mail('Needle : Current OHLC Saving Failure', "Error while fetching and saving current ohlc : {}".format(ex))

if __name__ == "__main__":
    
    SaveCurrentOhlc().save_ohlc()