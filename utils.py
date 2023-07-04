# ---------------------------------
# Utils class
# @author: Shi Junjie
# Fri 9 Jun 2023
# ---------------------------------

import os
import re
import pytz
import logging
import numpy as np
import pandas as pd
import datetime as dt
import pandas_market_calendars as mcal

from delta.logger import logger

est = pytz.timezone("America/New_York") # Eastern time

numpy_num_types = [
    np.int8, np.uint8, np.int16, np.uint16,
    np.int32, np.uint32, np.int64, np.uint64,
    np.float16,
    np.float32, np.float64,
]

desire_fmt = '%Y-%m-%d'

class Utils:

    def dt_obj_2_str(dates:list) -> list[str]:
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
        dates = [Utils()._convert_numpy_types(d) for d in dates]
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
                    dates_desire_fmt.append(dt.datetime.fromtimestamp(float(date), tz=est).strftime(desire_fmt))
                # is string datetime
                elif (len(str(date)) == 8):
                    dates_desire_fmt.append(dt.datetime.strptime(str(date), desire_fmt).strftime(desire_fmt))
                else:
                    raise ValueError("Unknown int pattern '{}'".format(date))
            # float
            elif (isinstance(date, dt_types[4])):
                dates_desire_fmt.append(dt.datetime.fromtimestamp(date, tz=est).strftime(desire_fmt))
            # other types
            else:
                raise ValueError("Unknown input type '{}', '{}'".format(type(date), date))
        return dates_desire_fmt

    def _convert_numpy_types(input_data):
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
    
    def all_trading_dts(start_date:str, end_date:str) -> tuple[list[str], list[int]]:
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
        schedule = nyse.schedule(start_date, end_date) # type: ignore
        trading_dates = [
            dt.datetime.strftime(d, desire_fmt)
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
        reference_dates:list, comparant_dates:list,
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
        logger.info('checking missing dts: {}(ref_dates), {}(ref_tss)'.format(
            len(reference_dates), len(reference_timestamps)
        ))
        logger.info('                   {}(cmp_dates), {}(cmp_tss)'.format(
            len(comparant_dates), len(comparant_timestamps)
        ))
        
        # construct missings
        missing_dates = list(set(reference_dates) - set(comparant_dates))
        missing_timestamps = list(set(reference_timestamps) - set(comparant_timestamps))
        
        logger.info('- missing dts: {}(dates) {}(tss)'.format(len(missing_dates), len(missing_timestamps)))
        
        return missing_dates, missing_timestamps
        
    def timestamp_periods(
        max_days_period:int, start_date:str,
        end_date:str,
    ) -> list[list]:
        """_summary_

        Args:
            max_days_period (int): _description_
            start_date (str): _description_
            end_date (str): _description_

        Returns:
            list[list]: _description_
        """
        i = 0
        is_last_period = False
        last_end_dt_obj = dt.datetime
        timestamps_periods = []
        
        # construct dt obj and check input timedelta
        start_dt_obj = dt.datetime.strptime(start_date, desire_fmt)
        end_dt_obj = dt.datetime.strptime(end_date, desire_fmt)
        start_end_days = (end_dt_obj - start_dt_obj).days      # days between start and end

        # check if exceed maximum period per api intra request
        if (start_end_days > max_days_period):
            # iter
            while(not is_last_period):
                if (i == 0):
                    current_dt_obj = start_dt_obj
                else:
                    current_dt_obj = last_end_dt_obj + dt.timedelta(days=1) # type: ignore
                future_dt_obj = current_dt_obj + dt.timedelta(days=max_days_period)
                future_end_days_perd = (future_dt_obj - end_dt_obj).days
                
                if (future_end_days_perd > 0):     # period future exceeds end date
                    timestamps_periods.append(
                        Utils()._construct_trading_period_timestamps(
                            current_dt_obj.strftime(desire_fmt), end_date
                        )
                    )
                    is_last_period = True
                else:       # not exceed next iteration
                    timestamps_periods.append(
                        Utils()._construct_trading_period_timestamps(
                            current_dt_obj.strftime(desire_fmt),
                            future_dt_obj.strftime(desire_fmt)
                        )
                    )
                    last_end_dt_obj = future_dt_obj

                i += 1    # increment by 1
            # return
            return timestamps_periods 
            
        else:
            return [Utils()._construct_trading_period_timestamps(start_date, end_date)]

    def _construct_trading_period_timestamps(
        start_date:str, end_date:str,
    ) -> list[int]:
        """_summary_

        Args:
            start_date (str): _description_
            end_date (str): _description_

        Returns:
            _type_: _description_
        """
        return [
            int(pd.Timestamp("{} 04:00:00".format(start_date), tz=est).timestamp()),    # pre-market
            int(pd.Timestamp("{} 20:00:00".format(end_date), tz=est).timestamp())    # after-hour
        ]
        
    def _check_save_error_tkls(error_tkls:list[str]):
        """choose to save error tickers

        Args:
            error_tkls (list[str]): tickers encountered error
        """

        save_error_tickers_info = "Do you want to save error tickers to a .csv file?\nYes or No(Y/N): "
        print(save_error_tickers_info)
        is_loop = True
        # check input
        while (is_loop):
            is_save_error_tkls = input().lower()
            if (is_save_error_tkls == "y"):
                file_path = Utils()._ask_4_path()
                pd.DataFrame(columns=['ticker'], data=np.array(error_tkls)).to_csv(file_path)
                logger.info('{}{}'.format(save_error_tickers_info, is_save_error_tkls))
                logger.info('error tickers saved to \'{}\''.format(file_path))
                print('error tickers saved to \'{}\''.format(file_path))
                is_loop = False
            elif (is_save_error_tkls == "n"):
                logger.info('{}{}'.format(save_error_tickers_info, is_save_error_tkls))
                is_loop = False
            else:
                continue

    def _ask_4_path() -> str:
        """ask file path to store error tickers

        Returns:
            str: file path
            
        """
        ask_4_path_info = "Plase enter full file path(.csv): "
        is_loop = True
        
        while (is_loop):
            file_path = input(ask_4_path_info)
            dir_path = os.path.dirname(file_path)

            # check valid path
            if (os.path.isdir(dir_path)):
                is_loop = False
                return file_path
            else:
                print("Directory path is invalid")

    def file_exists(file_path:str) -> bool:
        """check if file path exists

        Args:
            file_path (str): _description_

        Returns:
            bool: _description_
        """
        return os.path.isfile(file_path)

    def _format_column_names(
        df: pd.DataFrame, table_type:str,
    ) -> tuple[bool, pd.DataFrame]:
        """Formats the column names of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame whose column names need to be formatted.

        Returns:
            tuple[bool, pd.DataFrame]: A tuple containing a boolean value indicating
            whether the formatting was successful and the DataFrame with the
            formatted column names.
        """
        logger.info('- start to format columns for {} push'.format(table_type))
        # Create a new DataFrame to store the formatted column names
        formatted_df = pd.DataFrame()
        # check table types
        if (table_type == 'eod'):
            # Mapping of original eod column names to formatted column names
            columns = {
                'date': 'date_day',
                'open': 'd_open',
                'high': 'd_high',
                'low': 'd_low',
                'close': 'd_close',
                'volume': 'd_volume',
                'adjusted_close': 'd_adj_close',
            }
            exclude_cols = []
        elif (table_type == 'intra'):
            # Mapping of original intra column names to formatted column names
            columns = {
                'timestamp': 'date_time',
                'open': 'm_open',
                'high': 'm_high',
                'low': 'm_low',
                'close': 'm_close',
                'volume': 'm_volume',
            }
            exclude_cols = ['gmtoffset', 'datetime']
        elif (table_type == 'tkl_data'):
            # Mapping of original intra column names to formatted column names
            columns = {
                'Code': 'code',
                'Name': 'name',
                'Country': 'country',
                'Exchange': 'exchange',
                'Currency': 'currency',
                'Type': 'type',
                'IPODate': 'ipo_date',
            }
            exclude_cols = []
        else:
            logger.info('- fail to format, wrong table type: \'{}\''.format(table_type))
            return False, formatted_df
        
        # filter out un-want columns
        df = df.drop(columns=exclude_cols, axis=1)
        
        # Check if the keys of columns match the column names of the DataFrame
        if set(columns.keys()) != set(df.columns):
            logger.info('- fail to format, keys of columns do not match the column names of the df')
            return False, formatted_df
        
        # Iterate over the original column names and format them
        for col in df.columns:
            formatted_df[columns[col]] = df[col]
        
        # Return the DataFrame with the formatted column names
        return True, formatted_df 