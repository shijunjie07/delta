

import os
import re
import pytz
import sqlite3
import numpy as np
import pandas as pd
import datetime as dt


est = pytz.timezone("America/New_York")

numpy_int_types = [
    np.int8, np.uint8, np.int16, np.uint16,
    np.int32, np.uint32, np.int64, np.uint64,
]

numpy_float_types = [
    np.float16,
    np.float32, np.float64,
]

class Utils:
    
    def __init__(self):
        ...

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
        desire_fmt = '%Y-%m-%d'
        
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

    def _convert_numpy_types(self, input_data):
        """converts numpy types into float or int

        Args:
            input_data (_type_): date

        Returns:
            _type_: float or int or its original form
        """
        # check float
        for i in range(len(numpy_float_types)):
            if (isinstance(input_data, numpy_float_types[i])):
                return float(input_data)
        # check int
        for j in range(len(numpy_int_types)):
            if (isinstance(input_data, numpy_int_types[j])):
                return float(input_data)
        # if not numpy number types then return itself
        return input_data

# Utils.is_tkl_tables_exist('abc', os.viron["DB_PATH"])


dt_dt = [
    dt.datetime(2023, 1, 2, 3, 4, 13, 12).date(),
    dt.datetime(2023, 1, 2, 3, 4, 13, 12),
    dt.date.today(),
    dt.datetime(2023, 1, 2, 3, 4, 13, 12).timestamp(),
    dt.datetime(2023, 1, 2, 3, 4, 13, 12),
    # np.array([33]),
    1678924238,
    1673621057.235,
    np.float64(1655537841),
    np.int64(1662311855),
]
print([(x, type(x)) for x in dt_dt], end='\n--------------------------\n')
dt_ = Utils().dt_obj_2_str(dt_dt)
print([(x, type(x)) for x in dt_])