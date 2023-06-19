# -----------------------
# noDataDB class
# @author: Shi Junjie
# Wed 14 Jun 2023
# -----------------------

import re
import sqlite3
import logging

class NoDataDB:
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.NO_DATA_DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.NO_DATA_DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()
        self.exist_nodata_table_names = self._nodata_table_names()
        self.nodata_table_types = ['eod', 'intra']

    def is_nodata_table_exists(self, ticker:str) -> bool:
        """check if the input ticker exists in database

        Args:
            ticker (str): _description_

        Returns:
            bool: _description_
        """
 
 
        logging_info = 'check if {} nodata tables exist: '.format(ticker)
        crt_tables = []
        for table_type in self.nodata_table_types:
            table_name = '_'.join([ticker, table_type])
            if (table_name in self.exist_nodata_table_names):
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
        
    def crt_nodata_table(self, ticker:str, table_types:list[str]):
        """create no data table for a ticker

        Args:
            ticker (str): _description_
        """
        # create table
        if ('eod' in table_types):
            self.cur.execute("CREATE TABLE IF NOT EXISTS {}_eod(\
                date_day DATE UNIQUE);".format(ticker))
            self.con.commit()
        if ('intra' in table_types):
            self.cur.execute("CREATE TABLE IF NOT EXISTS {}_intra(\
                date_time DATETIME UNIQUE);".format(ticker))
            self.con.commit()
    
        logging_info = '- created {} nodata tables'.format(', '.join(table_types))
        self.logger.info(logging_info)

    def push_nodata_dts(
        self, ticker:str, dates:list[str], timestamps:list[int], 
    ):
        """push no data dts of a ticker from database

        Args:
            ticker (str): _description_
            dates (list): _description_
            timestamps (list): _description_
        """
        # check format
        self.logger.info("start to push nodata dts: {}(dates), {}(timestamps)".format(
            len(dates), len(timestamps)))
        is_right_fmt = self._check_dts_fmt(dates, timestamps)
        if (not is_right_fmt):
            raise ValueError("Invalid nodata dt formats")
        # construct list of tuples
        dates = [(d, ) for d in dates]
        timestamps = [(t, ) for t in timestamps]
        
        # push 
        # nodata the INSERT statement
        date_insert_query = "INSERT OR REPLACE INTO {}_eod (date_day) VALUES (?);".format(ticker)
        ts_insert_query = "INSERT OR REPLACE INTO {}_intra (date_time) VALUES (?);".format(ticker)
        
        # Execute the INSERT statement with the data
        self.cur.executemany(date_insert_query, dates)
        self.con.commit()

        self.cur.executemany(ts_insert_query, timestamps)
        self.con.commit()
        
    def _check_dts_fmt(
        self, dates:list[str], timestamps:list[int],
    ) -> bool:
        """check if dt format valid

        Args:
            dates (list[str]): _description_
            timestamps (list[int]): _description_

        Returns:
            bool: _description_
        """
        self.logger.info('-> checking dt formats')
        # check date fmt
        for date in dates:
            if (re.match(r'\d{4}-\d{2}-\d{2}', date)):
                continue
            else:
                self.logger.info(' - invalid date format for \'{}\''.format(date))
                return False
        
        # check timestamp fmt
        for timestamp in timestamps:
            if (len(str(timestamp)) >= 10):
                continue
            else:
                self.logger.info(' - invalid timestamp format for \'{}\''.format(timestamp))
                return False
        
        # return if all checks out
        self.logger.info('all dts format checks out')
        return True
    
    def pull_nodata_dts(
        self, ticker:str,
    ) -> tuple[list[str], list[int]]:
        """pull no data dts of a ticker from database

        Args:
            ticker (str): _description_

        Returns:
            tuple[list]: _description_
        """
        if (
            ('{}_eod'.format(ticker) in self.exist_nodata_table_names)
            and ('{}_intra'.format(ticker) in self.exist_nodata_table_names)
        ):
            date_rows = self.cur.execute("SELECT date_day FROM {}_eod".format(ticker))
            date_rows = self.cur.fetchall()
            ts_rows = self.cur.execute("SELECT date_time FROM {}_intra".format(ticker))
            ts_rows = self.cur.fetchall()
            return [x[0] for x in date_rows], [x[0] for x in ts_rows]
        else:
            self.logger.error('\'{}\' not in nodata db'.format(ticker))
            raise ValueError('\'{}\' not in nodata db'.format(ticker))

    def _nodata_table_names(self):
        """
        
        """
        rows = self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.cur.fetchall()
        return [x[0] for x in rows]