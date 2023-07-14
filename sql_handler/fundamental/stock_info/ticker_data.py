# -----------------------
# TickerDataHandler class
# @author: Shi Junjie
# Thu 13 Jul 2023
# -----------------------

import logging
import sqlite3
import pandas as pd

from delta.sql_handler.fundamental.stock_info.hist_market_cap import HistMarketCapHandler
from delta.utils import Utils


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


class TickerDataHandler(HistMarketCapHandler):
    
    def __init__(self, logger:logging.Logger, data_con:sqlite3.Connection, data_cur:sqlite3.Cursor, db_file_name:str, table_name:str):
        self.logger = logger
        self._stock_info_db_file_name = db_file_name
        
        self.data_con = data_con
        self.data_cur = data_cur
        self._ticker_data_table_name = table_name
        
        HistMarketCapHandler.__init__(self, self.logger, self.data_con, self.data_cur, self._stock_info_db_file_name)

    def crt_ticker_data_table(self):
        """create ticker data table
        """
        self.logger.info("create \'{}\' tables on \'{}\'".format(self._ticker_data_table_name, self._stock_info_db_file_name))
        try:
            crt_table_query = "CREATE TABLE IF NOT EXISTS {}(\
                    code TEXT UNIQUE, name TEXT, country TEXT, exchange TEXT,\
                    currency TEXT, type TEXT, ipo_date TEXT, mkt_cap_value BIGINT, mkt_cap TEXT);".format(self._ticker_data_table_name)
            self.data_cur.execute(crt_table_query)
            self.data_con.commit()
        except Exception as e:
            self.logger.info("- An exception occured while creating \'{}\' table: \'{}\', continue...".format(self._ticker_data_table_name, e))

        self.logger.info("- created \'{}\' table".format(self._ticker_data_table_name))
        

    def pull_ticker_data(self, tickers:list[str], keys:list[str]=None,) -> pd.DataFrame:
        """pull ticker data

        Args:
        -----
            tickers (list[str]): _description_
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
        self.logger.info("pull {} ticker data from \'{}\', table: \'{}\'".format(len(tickers), self._stock_info_db_file_name, self._ticker_data_table_name))
        try:
            if (keys):
                invalid_keys = [elem for elem in keys if elem not in ticker_data_keys]
                if (invalid_keys):
                    raise ValueError("keys element does not match with table column names: {}".format(', '.join(invalid_keys)))

            data_query = "SELECT * FROM {}".format(self._ticker_data_table_name)
            raw = self.data_cur.execute(data_query)
            data = raw.fetchall()
            self.logger.info("- pull \'{}\' success".format(self._ticker_data_table_name))
            
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
        

    def pull_tickers(self, mkt_cap:list=None) -> list[str]:
        """get tickers by market caps

        Args:
            mkt_cap (list, optional): market cap catagory. Defaults to None.

        Returns:
            _type_: list[str]
        """
        self.logger.info("pull tickers by market caps: {}".format(mkt_cap))
        
        data_query = "SELECT * FROM {}".format(self._ticker_data_table_name)
        raw = self.data_cur.execute(data_query)
        data = raw.fetchall()
        self.logger.info("- pull \'{}\' success".format(self._ticker_data_table_name))
        
        self.logger.info("- format df")
        columns = [desc[0] for desc in self.data_cur.description]
        df = pd.DataFrame(data, columns=columns)

        if (mkt_cap):
            df = df[df['mkt_cap'].isin(mkt_cap)]

        return df['code'].to_list()
    

    def push_ticker_data(self, df:pd.DataFrame) -> bool:
        """push ticker data

        Args:
            df (pd.DataFrame): data df

        Returns:
            bool: _description_
        """
        self.logger.info('prepare for ticker data push on \'{}\', table: \'{}\''.format(self._stock_info_db_file_name, self._ticker_data_table_name))
        # format column name
        is_success_format, df = Utils._format_column_names(df, self._ticker_data_table_name)
        if (not is_success_format):
            self.logger.info('- fail to format dataframe')
            return False
        
        # push data
        try:
            df.to_sql(self._ticker_data_table_name, self.data_con, if_exists='replace', index=False)
            self.data_con.commit()
            self.logger.info('success push {} ticker data on \'{}\', table: \'{}\''.format(
                len(df['code']), self._stock_info_db_file_name, self._ticker_data_table_name
            ))
            return True
        except Exception as e:
            self.logger.info('- {}'.format(e))
            return False