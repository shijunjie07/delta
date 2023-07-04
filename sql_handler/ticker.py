# -----------------------
# tickerDB class
# @author: Shi Junjie
# Wed 14 Jun 2023
# -----------------------

import pandas as pd
import sqlite3
import logging
from delta.utils import Utils

# import local
from delta.sql_handler.nodata import NoDataDB

class GetData:
    
    def __init__(self, logger:logging.Logger, DB_PATH:str, db_con, db_cur):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = db_con
        self.cur = db_cur
        
    def pull_tkl_dts(
        self, ticker:str, start_date:str,
        end_date:str, start_ts:int, end_ts:int
    ) -> tuple[list[str], list[int]]:
        """pull existing dates and timestamps from database
`
        Args:
            ticker (str): _description_
            start_date (str): _description_
            end_date (str): _description_
            start_ts (int): _description_
            end_ts (int): _description_

        Returns:
            tuple[list[str], list[int]]: _description_
        """

        self.logger.info("pull ticker dts: \'{}\'".format(ticker))
        self.logger.info("- {} {}; {} {}".format(
            start_date, end_date, start_ts, end_ts
        ))
        dt_obj_index = 0
        
        # queries
        eod_query = 'SELECT date_day FROM {}_eod WHERE date_day>={} AND date_day<={};'.format(
            ticker, start_date, end_date
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
        else:
            dates = []
        
        # extract timestamps
        if (raw_intra):
            timestamps = [int(x[dt_obj_index]) for x in raw_intra]
        else:
            timestamps = []

        self.logger.info('- existing dts: {}(date) {}(tss)'.format(len(dates), len(timestamps)))

        return dates, timestamps

class LoadData(
    NoDataDB,
):
    # TODO:
    # 1. add a check push dt function 
    #    to monitor nodata dt input
    #    if yes, rm nodata dt from nodata db
    #    else, push with out any changes
    # 2. 
    # 3. 
    # 4. 
    
    def __init__(self, logger:logging.Logger, DB_PATH:str, NO_DATA_DB_PATH:str, db_con, db_cur):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = db_con
        self.cur = db_cur
        
        # init NoDataDB
        NoDataDB.__init__(self, self.logger, NO_DATA_DB_PATH)

    def push_eod(self, ticker: str, df: pd.DataFrame) -> bool:
        """
        Pushes the data from a pandas DataFrame into the corresponding end-of-day (EOD) table in the database.

        Args:
            ticker (str): The ticker symbol of the stock or financial instrument.
            df (pd.DataFrame): The DataFrame containing the data to be pushed.

        Returns:
            bool: True if the data is successfully pushed, False otherwise.
        """
        self.logger.info('prepare for eod push')
        # format column name
        is_success_format, df = Utils()._format_column_names(df, 'eod')
        if (not is_success_format):
            self.logger.info('- fail to format dataframe')
            return False
        
        # push data
        try:
            df.to_sql('{}_eod'.format(ticker), self.con, if_exists='append', index=False)
            
            # check nodata timestamps
            is_success_rm = self._rm_nodata_dts(ticker, df['date_day'], [])
            if (not is_success_rm):
                self.logger.info('error occurred while preparing to push eod'.format(ticker))
                self.logger.info('- exception on \'_rm_nodata_dts\'')
                self.logger.info('- canceled commit')
                return False
            
            # commit if no exceptions
            self.con.commit()
            self.logger.info('success push eod')
            return True
        except Exception as e:
            self.logger.info('error occurred while pushing \'{}\' eod'.format(ticker))
            self.logger.info('- {}'.format(e))
            return False

    def push_intra(self, ticker:str, df:pd.DataFrame) -> bool:
        """
        Pushes the data from a pandas DataFrame into the corresponding intra-day table in the database.

        Args:
            ticker (str): The ticker symbol of the stock or financial instrument.
            df (pd.DataFrame): The DataFrame containing the data to be pushed.

        Returns:
            bool: True if the data is successfully pushed, False otherwise.
        """
        self.logger.info('prepare for intra push')

        # format column name
        is_success_format, df = Utils()._format_column_names(df, 'intra')
        if (not is_success_format):
            self.logger.info('- fail to format dataframe')
            return False
        
        # push data
        try:
            # push
            df.to_sql('{}_intra'.format(ticker), self.con, if_exists='append', index=False)
            
            # check nodata timestamps
            is_success_rm = self._rm_nodata_dts(ticker, [], df['date_time'])
            if (not is_success_rm):
                self.logger.info('error occurred while preparing to push intra'.format(ticker))
                self.logger.info('- exception on \'_rm_nodata_dts\'')
                self.logger.info('- canceled commit')
                return False
            
            # commit if no exceptions
            self.con.commit()
            self.logger.info('success push intra')
            return True
        except Exception as e:
            self.logger.info('error occurred while pushing \'{}\' intra'.format(ticker))
            self.logger.info('- {}'.format(e))
            return False

    def _rm_nodata_dts(self, ticker:str, dates:list[str], timestamps:list[int]):
        """remove nodata dts

        Args:
            ticker (str): _description_
            dates (list[str]): _description_
            timestamps (list[int]): _description_
            
        """
        self.logger.info("prepare to update nodata dts")
        nodata_dates, nodata_timestamps = self.pull_nodata_dts(ticker)
        
        rep_dates = list(set(dates) & set(nodata_dates))
        rep_timestamps = list(set(timestamps) & set(nodata_timestamps))
        
        # remove from nodata db
        is_success_rm = self.rm_dts(ticker, rep_dates, rep_timestamps)

        return is_success_rm

class TickerDB(GetData, LoadData):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str, NO_DATA_DB_PATH:str):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.logger.info(":: establish connection wtih findata.db ::")
        self.con = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()
        
        self.exist_ticker_table_names = self._ticker_table_names()
        self.table_types = ['eod', 'intra']
        
        GetData.__init__(self, self.logger, self.DB_PATH, self.con, self.cur)
        LoadData.__init__(self, self.logger, self.DB_PATH, NO_DATA_DB_PATH, self.con, self.cur)
        
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
            if (table_name in self.exist_ticker_table_names):
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

    def crt_tkl_tables(self, ticker:str, table_types:list[str]):
        """create ticker tables with the input table types

        Args:
            ticker (str): _description_
            table_types (str): _description_
        """
        # crt tables
        if ('eod' in table_types):
            #create eod
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_eod(\
                date_day DATE UNIQUE, d_open FLOAT, d_high FLOAT, d_low FLOAT, d_close FLOAT, d_volume BIGINT, d_adj_close FLOAT);")
            self.con.commit()
        if ('intra' in table_types):
            #create intra
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_intra(\
                date_time DATETIME UNIQUE, m_open FLOAT, m_high FLOAT, m_low FLOAT, m_close FLOAT, m_volume BIGINT);")
            self.con.commit()

        logging_info = 'created {} tables'.format(', '.join(table_types))
        self.logger.info(logging_info)

    def _ticker_table_names(self):
        """
        
        """
        rows = self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.cur.fetchall()
        return [x[0] for x in rows]