import os
import pandas as pd
import sqlite3
import logging
import datetime as dt


class DB:
    
    def __init__(self, logger:logging.Logger):
        self.logger = logger
        self.DB_PATH = os.environ['DB_PATH']
        self.con = sqlite3.connect(self.DB_PATH)
        self.cur = self.con.cursor()
        
        self.table_names = self._table_names()
        self.table_types = ['eod', 'intra']

    def get_mrkcap_tkls(self):
        """
        :return: a list of tickers
        """
        tickers = []
        for cap in self.market_caps:
            for ex in self.exchanges:
                if (ex == "us"):
                    tickers.extend(list(pd.read_csv(f"{self.TICKER_PATH}{cap}.csv")["Symbol"]))
                elif (ex == "nyse"):
                    tickers.extend(list(pd.read_csv(f"{self.TICKER_PATH}{cap}-nyse.csv")["Symbol"]))
        return [tkl.replace(" ", "") for tkl in tickers if (isinstance(tkl, str))]

    def is_tkl_tables_exist(self, ticker:str):
        logging_info = 'check if {} tables exist: '.format(ticker)
        crt_tables = []
        for table_type in self.table_types:
            table_name = '_'.join([ticker, table_type])
            print(table_name)
            if (table_name in self.table_names):
                continue
            else:
                # create table
                crt_tables.append(table_name)
        
        if (crt_tables):
            is_exist = False
        else:
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info)
        
        if (not is_exist):
            self._crt_tables(ticker, crt_tables)        # creat tables
        
    def pull_eod(self, tickers:list[str], dates_period:list):
        """_summary_

        Args:
            tickers (list[str]): _description_
            dates_period (list): [[start_date, end_date], [start_date, end_date]]
        """
        
    
    def _crt_tables(self, ticker:str, table_types:str):
        # crt tables
        if ('eod' in table_types):
            #create eod
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_eod(\
                date_day DATE UNIQUE, d_open FLOAT, d_high FLOAT, d_low FLOAT, d_close FLOAT, d_volume BIGINT);")
        if ('intra' in table_types):
            #create intra
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_intra(\
                date_time DATETIME UNIQUE, m_open FLOAT, m_high FLOAT, m_low FLOAT, m_close FLOAT, m_volume BIGINT);")

        self.logger.info(' - created {} tables'.format(', '.join(table_types)))


    def _table_names(self):
        """
        
        """
        rows = self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.cur.fetchall()
        return [x[0] for x in rows]
    
