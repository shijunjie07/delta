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
        self.logger.info(":: establish connection with nodata.db ::")
        self.nodata_con = sqlite3.connect(self.NO_DATA_DB_PATH, check_same_thread=False)
        self.nodata_cur = self.nodata_con.cursor()
        self.exist_nodata_table_names = self._nodata_table_names()
        self.nodata_table_types = ['eod', 'intra']


    def _check_connection(self):
        try:
            # Attempt to perform a simple operation to check the connection
            self.nodata_con.execute("SELECT 1")
            print(self.nodata_con)
            print("Database connection is active.")
        except sqlite3.Error:
            print("Database connection is not active.")
            
    def is_nodata_table_exists(self, ticker:str) -> tuple[bool, list[str]]:
        """check if the input ticker exists in database

        Args:
            ticker (str): _description_

        Returns:
            bool: _description_
        """
        self.logger.info("check nodata tables exist")
        logging_info = '- nodata tables exist: '.format(ticker)
        self.logger.info("- update \'exist_nodata_table_names\'")
        self.exist_nodata_table_names = self._nodata_table_names()
        crt_tables = []
        for table_type in self.nodata_table_types:
            table_name = '_'.join([ticker, table_type])
            if (table_name in self.exist_nodata_table_names):
                continue
            else:
                # append for later action: create table
                crt_tables.append(table_type)

        if (crt_tables):
            is_exist = False
        else:
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info)
        
        # return
        return is_exist, crt_tables
        
    def crt_nodata_tables(self, ticker:str, table_types:list[str]):
        """create no data table for a ticker

        Args:
            ticker (str): _description_
        """
        # create table
        self.logger.info("create nodata tables")
        is_crt = False
        if ('eod' in table_types):
            self.nodata_cur.execute("CREATE TABLE IF NOT EXISTS {}_eod(date_day DATE UNIQUE);".format(ticker))
            self.nodata_con.commit()
            is_crt = True
        if ('intra' in table_types):
            self.nodata_cur.execute("CREATE TABLE IF NOT EXISTS {}_intra(date_time DATETIME UNIQUE);".format(ticker))
            self.nodata_con.commit()
            is_crt = True

        if (is_crt):
            logging_info = '- created {} nodata tables'.format(', '.join(table_types))
        else:
            logging_info = '- fail to create nodata tables with \'table_type\': {}'.format(', '.join(table_types))
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
        self.logger.info("start to push nodata dts: {}(dates), {}(tss)".format(
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
        self.nodata_cur.executemany(date_insert_query, dates)
        self.nodata_con.commit()

        self.nodata_cur.executemany(ts_insert_query, timestamps)
        self.nodata_con.commit()
        
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
        self.logger.info('- checking dt formats')
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
            tuple[list]: dates, timestamps
        """
        self.logger.info("- pull nodata dts")
        self.logger.info("- update \'exist_nodata_table_names\'")
        self.exist_nodata_table_names = self._nodata_table_names()
        if (
            ('{}_eod'.format(ticker) in self.exist_nodata_table_names)
            and ('{}_intra'.format(ticker) in self.exist_nodata_table_names)
        ):
            date_rows = self.nodata_cur.execute("SELECT date_day FROM {}_eod".format(ticker))
            date_rows = self.nodata_cur.fetchall()
            ts_rows = self.nodata_cur.execute("SELECT date_time FROM {}_intra".format(ticker))
            ts_rows = self.nodata_cur.fetchall()
            self.logger.info("- pull success: {}(dates) {}(tss)".format(len(date_rows), len(ts_rows)))
            return [x[0] for x in date_rows], [x[0] for x in ts_rows]
        else:
            self.logger.error('- pull fail: \'{}\' not in nodata db'.format(ticker))
            raise ValueError('\'{}\' not in nodata db'.format(ticker))

    def rm_dts(self, ticker:str, dates:list[str], timestamps:list[int]) -> bool:
        """remove dts

        Args:
            ticker (str): _description_
            dates (list[str]): _description_
            timestamps (list[int]): _description_

        Returns:
            bool: is success rm
        """
        self.logger.info("- remove dts: {}(dates) {}(tss)".format(len(dates), len(timestamps)))
        
        if ((len(dates)==0) and (len(timestamps)==0)):
            self.logger.info("- empty input, pass remove dts")
            return True
    
        # rm query
        try:
            # table names
            eod_table_name, intra_table_name = '{}_eod'.format(ticker), '{}_intra'.format(ticker)
            # delete query
            eod_delete_query = "DELETE FROM {} WHERE date_day = ?".format(eod_table_name)
            intra_delete_query = "DELETE FROM {} WHERE date_time = ?".format(intra_table_name)
            
            # delete eod
            if (dates):
                self.nodata_cur.executemany(eod_delete_query, [(date, ) for date in dates])
                self.nodata_con.commit()
            
            # delete intra
            if (timestamps):
                self.nodata_cur.executemany(intra_delete_query, [(timestamp, ) for timestamp in timestamps])
                self.nodata_con.commit()
        
        except Exception as e:
            self.logger.info("- unable to remove dts: {}(dates) {}(tss)".format(len(dates), len(timestamps)))
            self.logger.info("- {}".format(e))
            return False

        # 
        self.logger.info("- remove dts success: {}(dates) {}(tss)".format(len(dates), len(timestamps)))
        
        return True
        

    def _nodata_table_names(self):
        """
        
        """
        rows = self.nodata_cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.nodata_cur.fetchall()
        return [x[0] for x in rows]