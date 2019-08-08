import pandas as pd

from LoggerWrapper import Logger
from Ohlc import Ohlc

class CompareMarket:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        
    def compare_ohlc(self):

        try:
            self.logger.debug("Fetching today's ohlc")
            self.current_ohlc_data = Ohlc().fetch_current_ohlc()
            ohlc_final_df = self.current_ohlc_data.rename(columns={'index': 'instrument', 'last_price':'last_price_today', 'open':'open_today', 'high':'high_today', 'low':'low_today', 'close':'close_today'})
            self.logger.info("Fetched today's ohlc")
        
            print("Below is today's ohlc data for {} companies:".format(len(ohlc_final_df)))
            print(ohlc_final_df)
            print("-----------------------------")

            try:
                self.logger.debug("Fetching yesterday's ohlc")
                self.prev_ohlc_data = pd.read_csv('ohlc_previous.csv')
                self.logger.info("Fetched yesterday's ohlc")
                
                try:
                    self.logger.debug("Comparing ohlc")
                    merged_df = ohlc_final_df.merge(self.prev_ohlc_data, how='left', on=['instrument'])
                    merged_df.drop(['instrument_token_x','instrument_token_y'], axis=1, inplace=True)
                    
                    if len(ohlc_final_df)!=len(self.prev_ohlc_data):
                        self.logger.error("Mismatch while comparing ohlc")
                        
                    print("Below is today's and yesterday's merged ohlc data for {} companies:".format(len(merged_df)))
                    print(merged_df)
                    print("-----------------------------")

                    to_trade_high = merged_df[(merged_df['open_today']>merged_df['high_prev']) & (merged_df['open_today']<(1.15 * merged_df['high_prev']) )]
                    to_trade_low = merged_df[(merged_df['open_today']<merged_df['low_prev']) & (merged_df['open_today']>(0.85 * merged_df['low_prev']))]

                    list_to_trade_high = list(to_trade_high['instrument'])
                    list_to_trade_low = list(to_trade_low['instrument'])

                    self.logger.info("Execute trade (high) on these {x} companies: {lis}".format(x=len(to_trade_high), lis=list_to_trade_high))
        
                    print("Execute trade (high) on these {x} companies: {lis}".format(x=len(to_trade_high), lis=list_to_trade_high))
                    print(to_trade_high[['instrument', 'high_prev', 'open_today']])
                    print("-----------------------------")
                    
                    self.logger.info("Execute trade (low) on these {x} companies: {lis}".format(x=len(to_trade_low), lis=list_to_trade_low))
                    
                    print("Execute trade (low) on these {x} companies: {lis}".format(x=len(to_trade_low), lis=list_to_trade_low))
                    print(to_trade_low[['instrument', 'low_prev', 'open_today']])
                    print("-----------------------------")

                    # Fetch ohlc of list of instruments in chunks and append as kite.ohlc does not give entire data at once : End

                    to_trade_high.to_csv('companies_high.csv', index=False)
                    to_trade_low.to_csv('companies_low.csv', index=False)
                    
                    self.logger.info("Compared ohlc and saved info about companies to trade")
                    
                except Exception as ex:
                    self.logger.error("Error while compared ohlc and saving info about companies to trade : {}".format(ex))
                    
            except Exception as ex:
                self.logger.error("Error in fetching yesterday's ohlc : {}".format(ex))
                
        except Exception as ex:
            self.logger.error("Error in fetching today's ohlc : {}".format(ex))
                
if __name__ == "__main__":
    
    CompareMarket().compare_ohlc()
    