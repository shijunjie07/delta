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

us_exg_pickle = 'us.pickle'
ticker_data_table_name = 'tkl_data'
ticker_data_db_file_name = 'data.db'

ticker_data_keys = {
    'code',
    'name',
    'country',
    'exchange',
    'currency',
    'type',
    'ipo_date',
    'mkt_cap',
}

class DataDB:
    
    def __init__(self, logger:logging.Logger, db_path:str):
        self.logger = logger
        self.ticker_data_db_file_path = db_path
        # sql connection
        self.logger.info(":: establish connection with data.db ::")
        self.con = sqlite3.connect(self.ticker_data_db_file_path)
        self.cur = self.con.cursor()
        
        # Utils().__init__(self, self.logger)
        
    def crt_ticker_data_table(self, table_name:str=ticker_data_table_name):
        """create ticker data table
        """
        self.logger.info("create \'{}\' tables on \'{}\'".format(table_name, ticker_data_db_file_name))
        try:
            crt_table_query = "CREATE TABLE IF NOT EXISTS {}(\
                    code TEXT UNIQUE, name TEXT, country TEXT, exchange TEXT,\
                    currency TEXT, type TEXT, ipo_date TEXT, mkt_cap TEXT);".format(table_name)
            self.cur.execute(crt_table_query)
            self.con.commit()
        except Exception as e:
            self.logger.info("- An exception occured while creating \'{}\' table: \'{}\', continue,,,".format(table_name, e))

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
        self.logger.info("pull {} ticker data from \'{}\', table: \'{}\'".format(len(tickers), ticker_data_db_file_name, table_name))
        try:
            if (keys):
                invalid_keys = [elem for elem in keys if elem not in ticker_data_keys]
                if (invalid_keys):
                    raise ValueError("keys element does not match with table column names: {}".format(', '.join(invalid_keys)))

            data_query = "SELECT * FROM {}".format(table_name)
            raw = self.cur.execute(data_query)
            data = raw.fetchall()
            self.logger.info("- pull \'{}\' success".format(table_name))
            
            self.logger.info("- format df")
            columns = [desc[0] for desc in self.cur.description]
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

    def push_ticker_data(self, df:pd.DataFrame, table_name:str=ticker_data_table_name) -> bool:
        """push ticker data

        Args:
            df (pd.DataFrame): data df

        Returns:
            bool: _description_
        """
        self.logger.info('prepare for ticker data push on \'{}\', table: \'{}\''.format(ticker_data_db_file_name, table_name))
        # format column name
        is_success_format, df = Utils._format_column_names(df, 'tkl_data')
        if (not is_success_format):
            self.logger.info('- fail to format dataframe')
            return False
        
        # push data
        try:
            df.to_sql('tkl_data', self.con, if_exists='replace', index=False)
            self.con.commit()
            self.logger.info('success push {} ticker data on \'{}\', table: \'{}\''.format(
                len(df['code']), ticker_data_db_file_name, ticker_data_table_name
            ))
            return True
        except Exception as e:
            self.logger.info('- {}'.format(e))
            return False

class FundDB(DataDB):

    def __init__(self, logger:logging.Logger, FUND_DIR_PATH:str):
        self.logger = logger
        self.FUND_DIR_PATH = FUND_DIR_PATH  # dir
        self.us_fund_file_path = '{}{}'.format(self.FUND_DIR_PATH, us_exg_pickle)
        self.ticker_data_db_file_path = '{}{}'.format(self.FUND_DIR_PATH, ticker_data_db_file_name)
        
        # make file
        # create dir
        if (not os.path.isdir(self.FUND_DIR_PATH)):
            self.logger.info("create dir \'{}\'".format(self.FUND_DIR_PATH))
            os.mkdir(self.FUND_DIR_PATH)
            self.logger.info("- created dir: \'{}\'".format(self.FUND_DIR_PATH))
        # crt us.pickle
        if (not os.path.isfile(self.us_fund_file_path)):
            self.logger.info("create pickle file in \'{}\'".format(self.FUND_DIR_PATH))
            open(self.us_fund_file_path, 'w').close()
            self.logger.info("- created file: \'{}\'".format(us_exg_pickle))
        # crt data.db
        if (not os.path.isfile(self.ticker_data_db_file_path)):
            self.logger.info("create data db file in \'{}\'".format(self.FUND_DIR_PATH))
            open(self.ticker_data_db_file_path, 'w').close()
            self.logger.info("- created file: \'{}\'".format(ticker_data_db_file_name))
        
        # init data.db
        DataDB.__init__(self, self.logger, self.ticker_data_db_file_path)


    def push_fund(self, data_dicts:list[dict], file_name:str='us') -> bool:
        """ush fundamentals

        Args:
            data_dicts (list[dict]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.

        Returns:
            bool: _description_
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
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

    def pull_fund(self, tickers:list[str], file_name:str='us', all:bool=False) -> tuple[bool, list[dict]]:
        """pull fundamentals

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.
            all (bool, optional): _description_. Defaults to False.

        Returns:
            tuple[bool, list[dict]]: list of ticker fundamentals dict
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
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

    def pull_ipo_dates_from_fud(self, tickers:list[str], file_name:str='us') -> tuple[bool, dict[str, str]]:
        """pull ipo dates from fundamentals

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.

        Returns:
            tuple[bool, dict[str, str]]: _description_
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
        self.logger.info("pull ipo dates from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))

        ipo_data = {}
        append_tickers = []
        # pull ticker dicts
        for data in loaded_data:
            tkl = data['General']['Code']
            if tkl in tickers:
                append_tickers.append(tkl)
                ipo_data['%s' % tkl] = str(data['General']['IPODate'])
            else:
                continue
        not_found_tickers = list(set(tickers) - set(append_tickers))

        self.logger.info("- return {} ticker ipo dates, expected {}, {} not found".format(
            len(append_tickers)), len(tickers), len(not_found_tickers)
        )
            
        return True, ipo_data


