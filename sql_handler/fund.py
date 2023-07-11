# -----------------------
# FundDB class
# @author: Shi Junjie
# Thu 29 Jun 2023
# -----------------------

import os
import pickle
import logging
import sqlite3
import pandas as pd

from delta.utils import Utils
from delta.sql_handler import us_exg_pickle
from delta.sql_handler import ticker_data_table_name
from delta.sql_handler import stock_info_db_file_name
from delta.sql_handler import hist_mktcap_table_name

# tkl_data table column names
ticker_data_keys = [
    'code',
    'name',
    'country',
    'exchange',
    'currency',
    'type',
    'ipo_date',
    'mkt_cap_value',
    'mkt_cap',
]

# market cap catagory
market_cap_ctgs = {
    "NANO": 0,
    "MICRO": 50_000_000,
    "SMALL": 300_000_000,
    "MEDIUM": 2_000_000_000,
    "LARGE": 10_000_000_000,
    "MEGA": 200_000_000_000,
}

class HistMarketCapHandler:
    # update market cap to 'tkl_data' table
    # push/pull market cap to '{ticker}_hist_mktcap' tables
    # crt ticker historical market cap table
    
    def __init__(self, logger:logging.Logger, data_con:sqlite3.Connection, data_cur:sqlite3.Cursor):
        self.logger = logger
        self.data_con = data_con
        self.data_cur = data_cur
    
    def crt_ticker_hist_mktcap_table(self, ticker:str):
        table_name = hist_mktcap_table_name.format(ticker)
        
    
    def pull_hist_mktcap(self, ticker:str, keys:list[str]=None) -> tuple[bool, dict]:
        table_name = hist_mktcap_table_name.format(ticker)
        self.logger.info("create \'{}\' tables on \'{}\'".format(table_name, stock_info_db_file_name))
        try:
            crt_table_query = "CREATE TABLE IF NOT EXISTS {}(trade_date DATETIME UNIQUE, mkt_cap_value BIGINT, mkt_cap TEXT);".format(table_name)
            self.data_cur.execute(crt_table_query)
            self.data_con.commit()
        except Exception as e:
            self.logger.info("- An exception occured while creating \'{}\' table: \'{}\', continue...".format(table_name, e))

        self.logger.info("- created \'{}\' table".format(table_name))    
    
    def push_hist_mktcap(self, ticker:str, df:pd.DataFrame) -> bool:
        table_name = hist_mktcap_table_name.format(ticker)
        ...
    
    def update_mktcap_2_tkldata(self) -> bool:
        ...

    def _cat_mktcap(value:int) -> str:
        """catagorise market cap

        Args:
            value (int): market cap values

        Raises:
            ValueError: _description_

        Returns:
            str: market cap
        """

        # check valid value
        positive_inf = float("inf")
        if not ((value >= 0) and (value < positive_inf)):
            raise ValueError("\'value\' should be a positive number, not \'{}\'".format(value))
        
        # find market cap
        for i in range(len(market_cap_ctgs.keys())):
            # determine threshold values
            lower_threshold_val = market_cap_ctgs[list(market_cap_ctgs.keys())[i]]

            # last one
            if (i == (len(market_cap_ctgs.keys())-1)):
                upper_threshold_val = positive_inf
            else:
                upper_threshold_val = market_cap_ctgs[list(market_cap_ctgs.keys())[i+1]]

            if ((value >= lower_threshold_val) and (value < upper_threshold_val)):
                return list(market_cap_ctgs.keys())[i]


class DataDB(HistMarketCapHandler):
    
    def __init__(self, logger:logging.Logger, db_path:str):
        self.logger = logger
        self.ticker_data_db_file_path = db_path
        # sql connection
        self.logger.info(":: establish connection with stock_info.db ::")
        self.data_con = sqlite3.connect(self.ticker_data_db_file_path)
        self.data_cur = self.data_con.cursor()
        
        HistMarketCapHandler.__init__(self, self.logger, self.data_con, self.data_cur)
    
    def is_data_table_exists(self, table_name:str=ticker_data_table_name) -> bool:
        """check if the input table name exists in data.db
           if checking historical market cap table use:
           >>> table_name=hist_mktcap_table_name.format(ticker)
           else it will check ticker_data_table_name

        Args:
            table_name (str, optional): table name. Defaults to ticker_data_table_name.

        Returns:
            bool: is table exists
        """

        self.logger.info("check data.db table exist")
        logging_info = '- \'{}\' table exist: '.format(table_name)
        
        is_exist = False
        if (table_name in self._data_table_names()):
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info)
        
        # return
        return is_exist
    
    def crt_ticker_data_table(self, table_name:str=ticker_data_table_name):
        """create ticker data table
        """
        self.logger.info("create \'{}\' tables on \'{}\'".format(table_name, stock_info_db_file_name))
        try:
            crt_table_query = "CREATE TABLE IF NOT EXISTS {}(\
                    code TEXT UNIQUE, name TEXT, country TEXT, exchange TEXT,\
                    currency TEXT, type TEXT, ipo_date TEXT, mkt_cap_value BIGINT, mkt_cap TEXT);".format(table_name)
            self.data_cur.execute(crt_table_query)
            self.data_con.commit()
        except Exception as e:
            self.logger.info("- An exception occured while creating \'{}\' table: \'{}\', continue...".format(table_name, e))

        self.logger.info("- created \'{}\' table".format(table_name))

    def pull_ticker_data(self, tickers:list[str], table_name:str=ticker_data_table_name, keys:list[str]=None,) -> pd.DataFrame:
        """pull ticker data

        Args:
        -----
            tickers (list[str]): _description_
            table_name (str, optional): _description_. Defaults to ticker_data_table_name.
            keys (list[str], optional): table columns. Defaults to None.

        Returns:
        --------
            pd.DataFrame: _description_

        Examples
        --------
        Constructing DataFrame from a dictionary.

        pull market caps
        >>> cols = ['mkt_cap']
        >>> tkls = ['AAPL', 'TSLA']
        >>> df = obj.pull_ticker_data(tickers=tkls, keys=cols)
        >>> df
        ticker mkt_cap
        AAPL     MEGA
        TSLA     MEGA
       
        """
        self.logger.info("pull {} ticker data from \'{}\', table: \'{}\'".format(len(tickers), stock_info_db_file_name, table_name))
        try:
            if (keys):
                invalid_keys = [elem for elem in keys if elem not in ticker_data_keys]
                if (invalid_keys):
                    raise ValueError("keys element does not match with table column names: {}".format(', '.join(invalid_keys)))

            data_query = "SELECT * FROM {}".format(table_name)
            raw = self.data_cur.execute(data_query)
            data = raw.fetchall()
            self.logger.info("- pull \'{}\' success".format(table_name))
            
            self.logger.info("- format df")
            columns = [desc[0] for desc in self.data_cur.description]
            df = pd.DataFrame(data, columns=columns)
            
            self.logger.info("- filter only tickers' data")
            df = df[df['code'].isin(tickers)]
            
            # filter keys columns
            if (keys):
                keys.append('code')
                self.logger.info("- generate subset df with columns: {}".format(', '.join(keys)))
                df = df[keys]

            self.logger.info("- done, length {}".format(df.shape[0]))
            return True, df
        except Exception as e:
            self.logger.info("- {}".format(e))
            return False, {'exception': e}

    def pull_tickers(self, table_name:str=ticker_data_table_name, mkt_cap:list=None) -> list[str]:
        """get tickers by market caps

        Args:
            table_name (str, optional): _description_. Defaults to ticker_data_table_name.
            mkt_cap (list, optional): market cap catagory. Defaults to None.

        Returns:
            _type_: list[str]
        """
        self.logger.info("pull tickers by market caps: {}".format(mkt_cap))
        
        data_query = "SELECT * FROM {}".format(table_name)
        raw = self.data_cur.execute(data_query)
        data = raw.fetchall()
        self.logger.info("- pull \'{}\' success".format(table_name))
        
        self.logger.info("- format df")
        columns = [desc[0] for desc in self.data_cur.description]
        df = pd.DataFrame(data, columns=columns)

        if (mkt_cap):
            df = df[df['mkt_cap'].isin(mkt_cap)]

        return df['code'].to_list()

    def push_ticker_data(self, df:pd.DataFrame, table_name:str=ticker_data_table_name) -> bool:
        """push ticker data

        Args:
            df (pd.DataFrame): data df

        Returns:
            bool: _description_
        """
        self.logger.info('prepare for ticker data push on \'{}\', table: \'{}\''.format(stock_info_db_file_name, table_name))
        # format column name
        is_success_format, df = Utils._format_column_names(df, table_name)
        if (not is_success_format):
            self.logger.info('- fail to format dataframe')
            return False
        
        # push data
        try:
            df.to_sql(table_name, self.data_con, if_exists='replace', index=False)
            self.data_con.commit()
            self.logger.info('success push {} ticker data on \'{}\', table: \'{}\''.format(
                len(df['code']), stock_info_db_file_name, ticker_data_table_name
            ))
            return True
        except Exception as e:
            self.logger.info('- {}'.format(e))
            return False
        
    def _data_table_names() -> list[str]:
        ...


class FundDB(DataDB):

    def __init__(self, logger:logging.Logger, FUND_DIR_PATH:str):
        self.logger = logger
        self.FUND_DIR_PATH = FUND_DIR_PATH  # dir
        self.us_fund_file_path = '{}{}'.format(self.FUND_DIR_PATH, us_exg_pickle)
        self.ticker_data_db_file_path = '{}{}'.format(self.FUND_DIR_PATH, stock_info_db_file_name)
        
        # init fund files
        self._init_fund_files()
        
        # init data.db
        DataDB.__init__(self, self.logger, self.ticker_data_db_file_path)


    def _init_fund_files(self):
        """init fund files
        """
        self.logger.info("init fund files")
        # create fundamental dir
        if (not os.path.isdir(self.FUND_DIR_PATH)):
            self.logger.info("- create dir \'{}\'".format(self.FUND_DIR_PATH))
            os.mkdir(self.FUND_DIR_PATH)
            
        # crt us.pickle
        if (not os.path.isfile(self.us_fund_file_path)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(us_exg_pickle, self.FUND_DIR_PATH))
            open(self.us_fund_file_path, 'w').close()
            
        # crt data.db
        if (not os.path.isfile(self.ticker_data_db_file_path)):
            self.logger.info("- create data \'{}\' file in \'{}\'".format(stock_info_db_file_name, self.FUND_DIR_PATH))
            open(self.ticker_data_db_file_path, 'w').close()

    
    def push_fund(self, data_dicts:list[dict], file_name:str=us_exg_pickle) -> bool:
        """ush fundamentals

        Args:
            data_dicts (list[dict]): _description_
            file_name (str, optional): _description_. Defaults to us_exg_pickle.

        Returns:
            bool: _description_
        """
        file_path = '{}{}'.format(self.FUND_DIR_PATH, file_name)
        self.logger.info("push fundamentals to \'{}\'".format(file_path))

        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
        
        # check valid data dict
        if (len(data_dicts) == 0):
            self.logger.info("- invalid \'data_dicts\', length: 0")
            return False

        # make unique 
        unique_data_dicts = list(set(data_dicts))
        self.logger.info("input: {} dicts, unique: {} dicts".format(
            len(data_dicts), len(unique_data_dicts)
        ))

        # append new data
        with open(file_path, 'ab') as pickle_file:
            pickle.dump(unique_data_dicts, pickle_file)

        self.logger.info("- success push {} dicts".format(len(unique_data_dicts)))
        return True

    def pull_fund(self, tickers:list[str], file_name:str=us_exg_pickle, all:bool=False) -> tuple[bool, list[dict]]:
        """pull fundamentals

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to us_exg_pickle.
            all (bool, optional): _description_. Defaults to False.

        Returns:
            tuple[bool, list[dict]]: list of ticker fundamentals dict
        """
        file_path = '{}{}'.format(self.FUND_DIR_PATH, file_name)
        self.logger.info("pull fundamentals from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))
    
        if (all):
            self.logger.info("- return all")
            return True, loaded_data
        else:
            data_dicts = []
            append_tickers = []
            # pull ticker dicts
            for data in loaded_data:
                tkl = data['General']['Code']
                if tkl in tickers:
                    append_tickers.append(tkl)
                    data_dicts.append(data)
                else:
                    continue
            not_found_tickers = list(set(tickers) - set(append_tickers))

            self.logger.info("- return {} ticker dicts, expected {}, {} not found".format(
                len(data_dicts)), len(tickers), len(not_found_tickers)
            )
            
            return True, data_dicts

    def pull_ipo_dates_from_fud(self, tickers:list[str], file_name:str=us_exg_pickle) -> tuple[bool, dict[str, str]]:
        """pull ipo dates from fundamentals

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to us_exg_pickle.

        Returns:
            tuple[bool, dict[str, str]]: _description_
        """
        file_path = '{}{}'.format(self.FUND_DIR_PATH, file_name)
        self.logger.info("pull ipo dates from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))

        ipo_date = {}
        append_tickers = []
        # pull ticker dicts
        for data in loaded_data:
            tkl = data['General']['Code']
            if tkl in tickers:
                append_tickers.append(tkl)
                ipo_date['%s' % tkl] = str(data['General']['IPODate'])
            else:
                continue
        not_found_tickers = list(set(tickers) - set(append_tickers))

        self.logger.info("- return {} ticker ipo dates, expected {}, {} not found".format(
            len(append_tickers)), len(tickers), len(not_found_tickers)
        )
            
        return True, ipo_date