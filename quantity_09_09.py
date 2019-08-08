import pandas as pd

from LoggerWrapper import Logger

class Quantity:
    
    def __init__(self):
        self.logger = Logger('trades.log', 'INFO').logging

    def decide_quantity(self):
        
        try:
            self.logger.debug('Loading required files')
            companies_high = pd.read_csv('companies_high.csv')
            companies_low = pd.read_csv('companies_low.csv')
            budget = pd.read_csv('leverage.csv')
            quantity_high = pd.read_csv('quantity_high.csv')
            quantity_low = pd.read_csv('quantity_low.csv')
            self.logger.info('Loaded required files')
            
            try:
                self.logger.debug('Calculating quantity')
                
                # Update ordering table (high) : Start

                for company in list(companies_high['instrument']):

                    previous_records = quantity_high[(quantity_high['instrument']==company)]
                    complete_records = previous_records[(previous_records['status']=='complete')]
                    last_record = complete_records[(complete_records['trade_number']==complete_records['trade_number'].max())]

                    if len(last_record)==0:
                        print('Trading with company {} for the very first time (high)'.format(company))

                        instrument = company
                        level = 1
                        high_prev = float(companies_high[companies_high['instrument']==company]['high_prev'])
                        open_today = float(companies_high[companies_high['instrument']==company]['open_today'])
                        daily_khwab = float(budget[budget['instrument']==company]['return']) * float(budget[budget['instrument']==company]['budget'])
                        actual_khwab = daily_khwab
                        quantity = round(actual_khwab/(float(budget[budget['instrument']==company]['return']) * open_today))
                        price = round(open_today + 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                        trigger_price = round(open_today + 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                        squareoff = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        stoploss = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        trade_number = 1
                        pl_tag = 'to_be_placed'
                        profit = 'to_be_placed'
                        adhoora_khwab = 'to_be_placed'
                        flag = 'to_be_placed'
                        profit_till_now = 'to_be_placed'
                        order_id = 'to_be_placed'
                        timestamp = 'to_be_placed'
                        status = 'to_be_placed'

                        new_data = {'instrument':[instrument],
                                    'level':[level],
                                    'high_prev':[high_prev],
                                    'open_today':[open_today],
                                    'daily_khwab':[daily_khwab],
                                    'actual_khwab':[actual_khwab],
                                    'quantity':[quantity],
                                    'price':[price],
                                    'trigger_price':[trigger_price],
                                    'squareoff':[squareoff],
                                    'stoploss':[stoploss],
                                    'trade_number':[trade_number],
                                    'pl_tag':[pl_tag],
                                    'profit':[profit],
                                    'adhoora_khwab':[adhoora_khwab],
                                    'flag':[flag],
                                    'profit_till_now':[profit_till_now],
                                    'order_id':[order_id],
                                    'timestamp':[timestamp],
                                    'status':[status]}

                        new_entry = pd.DataFrame(data=new_data)
                        quantity_high = quantity_high.append(new_entry, sort=False)
                        print(quantity_high)

                    else:

                        print('Trading with company {ins} with trade_number : {t_no} (high)'.format(ins=company,t_no=int(last_record['trade_number'])+1))

                        instrument = company
                        level = [1 if int(last_record['pl_tag'])==1 else int(last_record['level'])+1]
                        high_prev = float(companies_high[companies_high['instrument']==company]['high_prev'])
                        open_today = float(companies_high[companies_high['instrument']==company]['open_today'])
                        daily_khwab = [float(budget[budget['instrument']==company]['return'])*float(budget[budget['instrument']==company]['budget']) if last_record['flag']==1 else 0]
                        actual_khwab = float(last_record['adhoora_khwab']) + daily_khwab
                        quantity = round(actual_khwab/(float(budget[budget['instrument']==company]['return']) * open_today))
                        price = round(open_today + 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                        trigger_price = round(open_today + 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                        squareoff = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        stoploss = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        trade_number = int(last_record['trade_number'])+1
                        pl_tag = 'to_be_placed'
                        profit = 'to_be_placed'
                        adhoora_khwab = 'to_be_placed'
                        flag = 'to_be_placed'
                        profit_till_now = 'to_be_placed'
                        order_id = 'to_be_placed'
                        timestamp = 'to_be_placed'
                        status = 'to_be_placed'

                        new_data = {'instrument':[instrument],
                                    'level':[level],
                                    'high_prev':[high_prev],
                                    'open_today':[open_today],
                                    'daily_khwab':[daily_khwab],
                                    'actual_khwab':[actual_khwab],
                                    'quantity':[quantity],
                                    'price':[price],
                                    'trigger_price':[trigger_price],
                                    'squareoff':[squareoff],
                                    'stoploss':[stoploss],
                                    'trade_number':[trade_number],
                                    'pl_tag':[pl_tag],
                                    'profit':[profit],
                                    'adhoora_khwab':[adhoora_khwab],
                                    'flag':[flag],
                                    'profit_till_now':[profit_till_now],
                                    'order_id':[order_id],
                                    'timestamp':[timestamp],
                                    'status':[status]}

                        new_entry = pd.DataFrame(data=new_data)
                        quantity_high = quantity_high.append(new_entry, sort=False)
                        print(quantity_high) 

                quantity_high.to_csv('quantity_high.csv', index=False)

                # Update ordering table (high) : End


                # Update ordering table (low) : Start

                for company in list(companies_low['instrument']):

                    previous_records = quantity_low[(quantity_low['instrument']==company)]
                    complete_records = previous_records[(previous_records['status']=='complete')]
                    last_record = complete_records[(complete_records['trade_number']==complete_records['trade_number'].max())]

                    if len(last_record)==0:
                        print('Trading with company {} for the very first time (low)'.format(company))

                        instrument = company
                        level = 1
                        low_prev = float(companies_low[companies_low['instrument']==company]['low_prev'])
                        open_today = float(companies_low[companies_low['instrument']==company]['open_today'])
                        daily_khwab = float(budget[budget['instrument']==company]['return']) * float(budget[budget['instrument']==company]['budget'])
                        actual_khwab = daily_khwab
                        quantity = round(actual_khwab/(float(budget[budget['instrument']==company]['return']) * open_today))
                        price = round(open_today - 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                        trigger_price = round(open_today - 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                        squareoff = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        stoploss = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        trade_number = 1
                        pl_tag = 'to_be_placed'
                        profit = 'to_be_placed'
                        adhoora_khwab = 'to_be_placed'
                        flag = 'to_be_placed'
                        profit_till_now = 'to_be_placed'
                        order_id = 'to_be_placed'
                        timestamp = 'to_be_placed'
                        status = 'to_be_placed'

                        new_data = {'instrument':[instrument],
                                    'level':[level],
                                    'low_prev':[low_prev],
                                    'open_today':[open_today],
                                    'daily_khwab':[daily_khwab],
                                    'actual_khwab':[actual_khwab],
                                    'quantity':[quantity],
                                    'price':[price],
                                    'trigger_price':[trigger_price],
                                    'squareoff':[squareoff],
                                    'stoploss':[stoploss],
                                    'trade_number':[trade_number],
                                    'pl_tag':[pl_tag],
                                    'profit':[profit],
                                    'adhoora_khwab':[adhoora_khwab],
                                    'flag':[flag],
                                    'profit_till_now':[profit_till_now],
                                    'order_id':[order_id],
                                    'timestamp':[timestamp],
                                    'status':[status]}

                        new_entry = pd.DataFrame(data=new_data)
                        quantity_low = quantity_low.append(new_entry, sort=False)
                        print(quantity_low)

                    else:

                        print('Trading with company {ins} with trade_number : {t_no} (low)'.format(ins=company,t_no=int(last_record['trade_number'])+1))

                        instrument = company
                        level = [1 if int(last_record['pl_tag'])==1 else int(last_record['level'])+1]
                        low_prev = float(companies_low[companies_low['instrument']==company]['low_prev'])
                        open_today = float(companies_low[companies_low['instrument']==company]['open_today'])
                        daily_khwab = [float(budget[budget['instrument']==company]['return'])*float(budget[budget['instrument']==company]['budget']) if last_record['flag']==1 else 0]
                        actual_khwab = float(last_record['adhoora_khwab']) + daily_khwab
                        quantity = round(actual_khwab/(float(budget[budget['instrument']==company]['return']) * open_today))
                        price = round(open_today - 0.05 * max(round(20 * 0.0010 * open_today), 2), 2)
                        trigger_price = round(open_today - 0.05 * max(round(20 * 0.0005 * open_today),1), 2)
                        squareoff = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        stoploss = round((float(budget[budget['instrument']==company]['return'])) * price, 1)
                        trade_number = int(last_record['trade_number'])+1
                        pl_tag = 'to_be_placed'
                        profit = 'to_be_placed'
                        adhoora_khwab = 'to_be_placed'
                        flag = 'to_be_placed'
                        profit_till_now = 'to_be_placed'
                        order_id = 'to_be_placed'
                        timestamp = 'to_be_placed'
                        status = 'to_be_placed'

                        new_data = {'instrument':[instrument],
                                    'level':[level],
                                    'low_prev':[low_prev],
                                    'open_today':[open_today],
                                    'daily_khwab':[daily_khwab],
                                    'actual_khwab':[actual_khwab],
                                    'quantity':[quantity],
                                    'price':[price],
                                    'trigger_price':[trigger_price],
                                    'squareoff':[squareoff],
                                    'stoploss':[stoploss],
                                    'trade_number':[trade_number],
                                    'pl_tag':[pl_tag],
                                    'profit':[profit],
                                    'adhoora_khwab':[adhoora_khwab],
                                    'flag':[flag],
                                    'profit_till_now':[profit_till_now],
                                    'order_id':[order_id],
                                    'timestamp':[timestamp],
                                    'status':[status]}

                        new_entry = pd.DataFrame(data=new_data)
                        quantity_low = quantity_low.append(new_entry, sort=False)
                        print(quantity_low) 

                quantity_low.to_csv('quantity_low.csv', index=False)

                # Update ordering table (low) : End
                self.logger.info('Calculated quantity')
                
            except Exception as ex:
                self.logger.error('Error in calculating quantity : {}'.format(ex))
                
        except Exception as ex:
            self.logger.error('Error in loading required files : {}'.format(ex))                
                
if __name__ == "__main__":
    
    Quantity().decide_quantity()        
        