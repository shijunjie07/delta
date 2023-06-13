import os
import pandas as pd
import sqlite3
import logging
import datetime as dt


class DB:
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()
        
        self.table_names = self._table_names()
        self.table_types = ['eod', 'intra']

    def get_mrkcap_tkls(self, ticker_path:str, market_caps:list[str]):
        """
        :return: a list of tickers
        """
        tickers = []
        for cap in market_caps:
                    tickers.extend(list(pd.read_csv(f"{ticker_path}{cap}.csv")["Symbol"]))
        return [tkl.replace(" ", "") for tkl in tickers if (isinstance(tkl, str))]

    def is_tkl_tables_exist(self, ticker:str) -> tuple[bool, list[str]]:
        """check if the input ticker exists in database

        Args:
            ticker (str): _description_

        Returns:
            tuple[bool, list[str]]: _description_
        """
 
        logging_info = 'check if {} tables exist: '.format(ticker)
        crt_tables = []
        for table_type in self.table_types:
            table_name = '_'.join([ticker, table_type])
            if (table_name in self.table_names):
                continue
            else:
                # append for later action: create table
                crt_tables.append(table_name)

        if (crt_tables):
            is_exist = False
        else:
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info) 
        
        # return
        return is_exist, crt_tables
        
    def pull_eod(self, tickers:list[str], dates_period:list):
        """_summary_

        Args:
            tickers (list[str]): _description_
            dates_period (list): [[start_date, end_date], [start_date, end_date]]
        """
        ...

    def crt_tables(self, ticker:str, table_types:list[str]):
        """create ticker tables with the input table types

        Args:
            ticker (str): _description_
            table_types (str): _description_
        """
        # crt tables
        if ('eod' in table_types):
            #create eod
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_eod(\
                date_day DATE UNIQUE, d_open FLOAT, d_high FLOAT, d_low FLOAT, d_close FLOAT, d_volume BIGINT);")
        if ('intra' in table_types):
            #create intra
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_intra(\
                date_time DATETIME UNIQUE, m_open FLOAT, m_high FLOAT, m_low FLOAT, m_close FLOAT, m_volume BIGINT);")

        logging_info = ' - created {} tables'.format(', '.join(table_types))
        self.logger.info(logging_info)


    def _table_names(self):
        """
        
        """
        rows = self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.cur.fetchall()
        return [x[0] for x in rows]
    
    def pull_tkl_dts(
        self, ticker:str, start_date:str,
        end_date:str, start_ts:int, end_ts:int
    ) -> tuple[list[str], list[int]]:
        """pull existing dates and timestamps from database

        Args:
            ticker (str): _description_
            start_date (str): _description_
            end_date (str): _description_
            start_ts (int): _description_
            end_ts (int): _description_

        Returns:
            tuple[list[str], list[int]]: _description_
        """
    
        dt_obj_index = 0
        
        # queries
        eod_query = 'SELECT date_day FROM {}_eod WHERE date_day>={} AND date_day<={};'.format(
            ticker, start_date.replace('-', ''), end_date.replace('-', '')
        )
        intra_query = 'SELECT date_time FROM {}_intra WHERE date_time>={} AND date_time<={};'.format(
            ticker, start_ts, end_ts
        )
        
        # pull from database
        raw_eod = self.con.execute(eod_query).fetchall()    # eod date "%Y%m%d"
        raw_intra = self.con.execute(intra_query).fetchall() # intra datetime unix timestamp
        
        # extract dates
        if (raw_eod):
            dates = [str(x[dt_obj_index]) for x in raw_eod]
            dates = [
                '{}-{}-{}'.format(x[:4], x[:6], x[6:])
                for x in dates
            ]
        else:
            dates = []
        
        # extract timestamps
        if (raw_intra):
            timestamps = [int(x[dt_obj_index]) for x in raw_intra]
        else:
            timestamps = []

        return dates, timestamps