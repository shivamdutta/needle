import pandas as pd

from LoggerWrapper import Logger
from Mailer import Mailer
from LogIn import LogIn

class Ohlc:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging
        self.kite = LogIn().return_kite_obj()
        self.mailer = Mailer()
        
    def fetch_current_ohlc(self):
        
        # Generate list of instruments to trade on : Start

        leverage_companies_only = True

        if leverage_companies_only:
            leverage_df = pd.read_csv('leverage.csv')
            list_of_instruments = list(leverage_df['instrument'])
        else:

            try:
                companies = self.kite.instruments(self.kite.EXCHANGE_NSE)
                companies_df = pd.DataFrame(companies)
                self.logger.info("Fetched list of {} NSE companies".format(len(companies_df)))
            except Exception as ex:
                self.logger.error("Failed to fetch data of listed NSE companies : {}".format(ex))
            
            if len(companies_df['exchange'].unique()) == 1:
                companies_df['instruments'] = 'NSE:' + companies_df['tradingsymbol'].astype(str)
            else:
                self.logger.error('All companies are not NSE listed')
            list_of_instruments = list(companies_df['instruments'])

        # Generate list of instruments to trade on : End
        
        self.logger.debug("Fetching ohlc data for {} companies".format(len(list_of_instruments)))

        chunk_size = 50

        ohlc_final_df = pd.DataFrame()

        for i in range(0, len(list_of_instruments), chunk_size):

            list_chunk = list_of_instruments[i:i+chunk_size]
            ohlc_data = self.kite.ohlc(list_chunk)
            ohlc_df = pd.DataFrame.from_dict(ohlc_data, orient='index')
            ohlc_df['open'] = [d.get('open') for d in ohlc_df.ohlc]
            ohlc_df['high'] = [d.get('high') for d in ohlc_df.ohlc]
            ohlc_df['low'] = [d.get('low') for d in ohlc_df.ohlc]
            ohlc_df['close'] = [d.get('close') for d in ohlc_df.ohlc]

            ohlc_final_df = ohlc_final_df.append(ohlc_df, ignore_index=False)

        ohlc_final_df.reset_index(inplace=True)
        ohlc_final_df.drop(['ohlc'], axis=1, inplace=True)
        
        self.logger.info("Fetched ohlc data for {} companies : {}".format(len(ohlc_final_df), list(ohlc_final_df['index'])))
        
        if len(list_of_instruments)-len(ohlc_final_df):
            self.logger.error("Could not fetch ohlc data for {x} out of {y} companies : {lis}".format(x = len(list_of_instruments)-len(ohlc_final_df), y = len(list_of_instruments), lis = list(set(list_of_instruments) - set(list(ohlc_final_df['index'])))))
            self.mailer.send_mail('Needle : OHLC Fetching Failure', "Could not fetch ohlc data for {x} out of {y} companies : {lis}".format(x = len(list_of_instruments)-len(ohlc_final_df), y = len(list_of_instruments), lis = list(set(list_of_instruments) - set(list(ohlc_final_df['index'])))))
            
        return ohlc_final_df