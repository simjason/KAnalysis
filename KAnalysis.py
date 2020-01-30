import os
import pandas as pd
import numpy as np
import ffn
import ta
import bt
import yaml

from tiingo import TiingoClient
from datetime import date
ffn.core.extend_pandas()


##########################################################################################
#                                    Portfolio Class                                     #
##########################################################################################

# CovarianceShrinkage(monthly_indicies,frequency = 12).ledoit_wolf()








##########################################################################################
#                                       Signal Class                                     #
##########################################################################################

RISK_OFF = 0
RISK_ON  = 1

class Signal:

    def __init__(self, signal_file_path : str):

        self.name, self.signals = self.load_signals(signal_file_path)
        self.daily_signals = self.get_daily_risk_on_off(self.signals)

    def load_signals(self, signal_file_path):
        """

        """
        with open(signal_file_path, 'r') as file:

            signal_name = file.readline()

            signal_frame = pd.read_csv(file,sep = ',', 
                                            header = None, 
                                            index_col = 1, 
                                            parse_dates = True)

            # Ensure first column has B and S
            assert len(signal_frame[0][ (signal_frame[0] != 'B') & 
                                        (signal_frame[0] != 'S') ]) == 0

            # Ensure second column has valid dates
            signal_frame.index.name = 'Dates'
            signal_frame.name = signal_name

        return signal_name, signal_frame[0]

    def get_daily_risk_on_off(self, buy_sell_signals : pd.Series):
        """

        """
        risk_periods = []
        buy_sell_signals.loc[pd.datetime.today()] = buy_sell_signals.iloc[-1]

        for i in range(1,len(buy_sell_signals)):

            risk_period = np.arange(buy_sell_signals.index[i-1], 
                                    buy_sell_signals.index[i],
                                    dtype='datetime64[D]')

            assert ((buy_sell_signals[i] == 'B' or buy_sell_signals[i] == 'S') and
                    (buy_sell_signals[i-1] == 'B' or buy_sell_signals[i-1] == 'S')) 

            if buy_sell_signals[i-1] == 'B':

                risk_periods.append(
                    pd.Series( np.full( shape = len(risk_period), 
                                        fill_value = RISK_ON),
                                        index = risk_period))

            else:

                risk_periods.append(
                    pd.Series( np.full( shape = len(risk_period), 
                                        fill_value = RISK_OFF),
                                        index = risk_period))

        risk_settings = pd.concat(risk_periods)
        risk_settings.name = self.name
        return risk_settings

    def apply_to(self, risk_on: pd.Series, risk_off: pd.Series):
        """

        """
        assert (isinstance(risk_on, pd.Series)) and (isinstance(risk_off, pd.Series))

        risk_on.name = "Risk On"
        risk_off.name = "Risk Off"

        joined_data = pd.concat([   risk_on , risk_off , self.daily_signals],
                                    join = 'inner', axis = 1)


        risk_on_series = joined_data[joined_data[self.name]==RISK_ON]['Risk On']
        risk_off_series = joined_data[joined_data[self.name]==RISK_OFF]['Risk Off']

        return_series = pd.concat([risk_on_series,risk_off_series]).sort_index(ascending = True)
        return_series.name = self.name + ' Return Series'

        return return_series

##########################################################################################
#                                       Signal Class                                     #
##########################################################################################

class Table():

    def __init__(self):

        self.table = pd.DataFrame()
        self.columns = self.table.columns
        self.config = yaml.load(open(r'config.yaml'), Loader=yaml.FullLoader)    

        
        # Init Tiingo Client
        self.client = TiingoClient(self.config['tiingo'])

        # Self Directory

    def add_Column(self, name , column):

        self.table = pd.concat([self.table,column], axis = 1)

    def fill_column(self, name , value):

        self.table[name] = value

    def generate_signal(self, column):
        """
        
        """
        pass

    def read_metastock(self, file_name):
        """

        """
        pass

    def read_csv(self, file_name):
        """

        """
        pass


    def retrieve(self, ticker: str, frequency: str = None,
                                    startDate: str = None, 
                                    endDate: str   = None, 
                                    adjusted = True):
        """

        """
        # Init Parameters
    
        metadata = self.client.get_ticker_metadata(ticker,fmt='object')
        startDate = metadata.startDate if startDate == None else startDate
        endDate = metadata.endDate if endDate == None else endDate
        frequency = 'daily' if frequency == None else frequency

        if startDate < metadata.startDate:
            raise DateError('Value entered for startDate is less than available ticker history') 

        if startDate < metadata.startDate:
            raise DateError('Value entered for startDate is less than available ticker history') 

        valid_freq = ['daily','weekly','monthly','annually']

        if frequency not in valid_freq:
            raise DateError('frequency entered is not a valid frequency' + str(valid_freq)) 

        dataframe = self.client.get_dataframe(ticker,
                                         startDate = startDate,
                                         endDate = endDate,
                                         frequency = 'daily')

        if adjusted:
            dataframe.drop(['open','high','low','close','volume','divCash','splitFactor'], 
                axis = 1, 
                inplace = True)

            dataframe.rename({  'adjOpen':'open',
                                'adjHigh':'high',
                                'adjLow':'low',
                                'adjClose':'close',
                                'adjVolume':'volume'},
                                axis = 1, 
                                inplace = True)
        else:            
            dataframe.drop(['adjOpen','adjHigh','adjLow','adjClose','adjVolume'], 
                axis = 1,
                inplace = True)


        self.table = pd.concat([dataframe, self.table], join = 'outer', axis = 1)    
        self.columns = list(dataframe.columns)


        

##########################################################################################
#                                       Data Class                                       #
##########################################################################################

class DateError(ValueError):
    pass


if __name__ == '__main__':

    spy = pd.read_csv('SPY.CSV', index_col = 0, parse_dates = True)[' Close'].to_returns().dropna()
    agg = pd.read_csv('AGG.CSV', index_col = 0, parse_dates = True)[' Close'].to_returns().dropna()

    sig = Signal('sig_files/NuAggHY2_30.sig')
    sig.apply_to(spy,agg)

    key.set_contents_from_string(csv_buffer.getvalue())
