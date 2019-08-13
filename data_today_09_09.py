import pandas as pd

from LoggerWrapper import Logger
from Mailer import Mailer
from Ohlc import Ohlc

class CompareMarket:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        self.mailer = Mailer()
        
    def compare_ohlc(self):

        try:
            self.logger.debug("Fetching today's ohlc")
            self.current_ohlc_data = Ohlc().fetch_current_ohlc()
            ohlc_final_df = self.current_ohlc_data.rename(columns={'index': 'instrument', 'last_price':'last_price_today', 'open':'open_today', 'high':'high_today', 'low':'low_today', 'close':'close_today'})
            self.logger.info("Fetched today's ohlc")

            try:
                self.logger.debug("Fetching yesterday's ohlc")
                self.prev_ohlc_data = pd.read_csv('ohlc_previous.csv')
                self.logger.info("Fetched yesterday's ohlc")
                
                try:
                    self.logger.debug("Comparing ohlc and saving info about companies to trade")
                    merged_df = ohlc_final_df.merge(self.prev_ohlc_data, how='left', on=['instrument'])
                    merged_df.drop(['instrument_token_x','instrument_token_y'], axis=1, inplace=True)
                    
                    if len(ohlc_final_df)!=len(self.prev_ohlc_data):
                        self.logger.error("Mismatch while comparing ohlc")

                    to_trade_high = merged_df[(merged_df['open_today']>merged_df['high_prev']) & (merged_df['open_today']<(1.15 * merged_df['high_prev']) )]
                    to_trade_low = merged_df[(merged_df['open_today']<merged_df['low_prev']) & (merged_df['open_today']>(0.85 * merged_df['low_prev']))]

                    list_to_trade_high = list(to_trade_high['instrument'])
                    list_to_trade_low = list(to_trade_low['instrument'])

                    self.logger.info("Execute trade (high) on these {x} companies: {lis}".format(x=len(to_trade_high), lis=list_to_trade_high))
                    
                    self.logger.info("Execute trade (low) on these {x} companies: {lis}".format(x=len(to_trade_low), lis=list_to_trade_low))
                    
                    to_trade_high.to_csv('companies_high.csv', index=False)
                    to_trade_low.to_csv('companies_low.csv', index=False)
                    
                    self.logger.info("Compared ohlc and saved info about companies to trade")
                    self.mailer.send_mail('Needle : OHLC Comparison Done Successfully', "OHLC Comparison Table : <br>" + merged_df.to_html() + "Companies for trading (High) : <br>" + to_trade_high.to_html() + "Companies for trading (Low) : <br>" + to_trade_low.to_html())
                    
                except Exception as ex:
                    self.logger.error("Error while compared ohlc and saving info about companies to trade : {}".format(ex))
                    self.mailer.send_mail('Needle : OHLC Comparison Failure', "Error while compared ohlc and saving info about companies to trade : {}".format(ex))
                    
            except Exception as ex:
                self.logger.error("Error in fetching yesterday's ohlc : {}".format(ex))
                self.mailer.send_mail('Needle : OHLC Comparison Failure', "Error in fetching yesterday's ohlc : {}".format(ex))
                
        except Exception as ex:
            self.logger.error("Error in fetching today's ohlc : {}".format(ex))
            self.mailer.send_mail('Needle : OHLC Comparison Failure', "Error in fetching today's ohlc : {}".format(ex))
            
if __name__ == "__main__":
    
    CompareMarket().compare_ohlc()
    