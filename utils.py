# ---------------------------------
# Utils
# @author: Shi Junjie
# Fri 9 Jun 2023
# ---------------------------------

import os
import re
import pytz
import sqlite3
import logging
import numpy as np
import pandas as pd
import datetime as dt

import pandas_market_calendars as mcal

est = pytz.timezone("America/New_York") # Eastern time

numpy_num_types = [
    np.int8, np.uint8, np.int16, np.uint16,
    np.int32, np.uint32, np.int64, np.uint64,
    np.float16,
    np.float32, np.float64,
]


class Utils:
    
    def __init__(self, logger:logging.Logger):
        self.logger = logger
        self.desire_fmt = '%Y-%m-%d'

        
    def dt_obj_2_str(self, dates:list) -> list[str]:
        """convert the list of input into string with format "%Y-%m-%d"

        Args:
            dates (list): list of dates with various formats
            

        Returns:
            list[str]: list of strings with format "%Y-%m-%d"
        """
        dt_types = [
            dt.datetime,
            dt.date,
            str,
            int,
            float,
        ]
        # check numpy types
        dates = [self._convert_numpy_types(d) for d in dates]
        dates_desire_fmt = []
        # convert to str
        for date in dates:
            # dt.datetime
            if (isinstance(date, dt_types[0])):
                dates_desire_fmt.append(dt.datetime.strftime(date, '%Y-%m-%d'))
            # dt.date
            elif (isinstance(date, dt_types[1])):
                dates_desire_fmt.append(str(date))
            # str
            elif (isinstance(date, dt_types[2])):
                # chcek pattern
                # %Y-%m-%d
                if (re.match(r'\d{4}-\d{2}-\d{2}', date)):
                    dates_desire_fmt.append(date)
                elif (re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date)):
                    dates_desire_fmt.append(date[:10])
                elif (re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', date)):
                    dates_desire_fmt.append(date[:10])
                else:
                    raise ValueError("Unknown string pattern '{}'".format(date))
            # int
            elif (isinstance(date, dt_types[3])):
                # is timestamp
                if (len(str(date)) >= 10):
                    dates_desire_fmt.append(dt.datetime.fromtimestamp(float(date), tz=est).strftime(self.desire_fmt))
                # is string datetime
                elif (len(str(date)) == 8):
                    dates_desire_fmt.append(dt.datetime.strptime(str(date), self.desire_fmt).strftime(self.desire_fmt))
                else:
                    raise ValueError("Unknown int pattern '{}'".format(date))
            # float
            elif (isinstance(date, dt_types[4])):
                dates_desire_fmt.append(dt.datetime.fromtimestamp(date, tz=est).strftime(self.desire_fmt))
            # other types
            else:
                raise ValueError("Unknown input type '{}', '{}'".format(type(date), date))
        return dates_desire_fmt

    def _convert_numpy_types(self, input_data):
        """converts numpy types into float or int

        Args:
            input_data (_type_): date

        Returns:
            _type_: float or int or its original form
        """
        # check types
        for i in range(len(numpy_num_types)):
            if (isinstance(input_data, numpy_num_types[i])):
                return float(input_data)
        # if not numpy number types then return itself
        return input_data
    
    
    def all_trading_dts(self, start_date:str, end_date:str) -> tuple[list[str], list[int]]:
        """construct all trading dates and timestamps between start and end dates
           * only call this function once *

        Args:
            start_date (str): _description_
            end_date (str): _description_

        Returns:
            tuple[list[str], list[int]]: _description_
        """

        # construct dates between input dates
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date, end_date)
        trading_dates = [
            dt.datetime.strftime(d, self.desire_fmt)
            for d in schedule.index.normalize().tolist()
        ]
        
        timestamps = []
        # construct timestamps between 09:00:00 and 20:00:00 for each trading date
        for trading_date in trading_dates:
            # Define the pre-market start and after-hours end times
            pre_market_start = pd.Timestamp("{} 04:00:00".format(trading_date), tz=est)
            after_hours_end = pd.Timestamp("{} 20:00:00".format(trading_date), tz=est)

            # print(timestamps[0].timestamp(), int(timestamps[0].timestamp()))
            timestamps.extend([
                int(t.timestamp())
                for t in pd.date_range(
                    start=pre_market_start, end=after_hours_end, freq='1min'    # generate all 1min dt objs
                )
            ])
        
        return trading_dates, timestamps

    def missing_dts(
        self, reference_dates:list, comparant_dates:list,
        reference_timestamps:list, comparant_timestamps:list
    ) -> tuple[list[str], list[int]]:
        """construct missing dates and timestamps

        Args:
            reference_dates (list): _description_
            comparant_dates (list): _description_
            reference_timestamps (list): _description_
            comparant_timestamps (list): _description_

        Returns:
            tuple[list[str], list[int]]: _description_
        """
        
        # construct missings
        missing_dates = list(set(reference_dates) - set(comparant_dates))
        missing_timestamps = list(set(reference_timestamps) - set(comparant_timestamps))
        
        return missing_dates, missing_timestamps
        
        
    
    
    # def convert_unix(start_time:str, end_time:str):
        
    #     # print(start_time)
    #     local_time = pytz.timezone("America/New_York")
    #     from_temp = start_time
    #     naive_0_from_temp = dt.datetime.strptime(from_temp, "%Y-%m-%d %H:%M:%S")
    #     local_0_from_temp = local_time.localize(naive_0_from_temp, is_dst=None)
    #     utc_0_from_temp = local_0_from_temp.astimezone(pytz.utc).timestamp()
    #     start_timestamp = int(utc_0_from_temp)

    #     to_temp = end_time
    #     naive_1_to_temp = dt.datetime.strptime(to_temp, "%Y-%m-%d %H:%M:%S")
    #     local_1_to_temp = local_time.localize(naive_1_to_temp, is_dst=None)
    #     utc_1_to_temp = local_1_to_temp.astimezone(pytz.utc).timestamp()
    #     end_timestamp = int(utc_1_to_temp)
        
    #     return [start_timestamp, end_timestamp]
    