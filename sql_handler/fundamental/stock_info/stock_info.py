# -----------------------
# init class
# @author: Shi Junjie
# Thu 13 Jul 2023
# -----------------------

import logging
import sqlite3

from delta.sql_handler.fundamental.stock_info.ticker_data import TickerDataHandler

hist_mktcap_table_name = '{}_hist_mktcap'       # historical market capitalisation of a ticker
stock_info_db_file_name = 'stock_info.db'       # stock info db name
ticker_data_table_name = 'tkl_data'             # snapshot of ticker info


class StockInfoDB(TickerDataHandler):
    
    def __init__(self, logger:logging.Logger, FUND_DIR_PATH:str):
        self.logger = logger
        self._stock_info_db_file_name = stock_info_db_file_name
        self._ticker_data_table_name = ticker_data_table_name
        self.FUND_DIR_PATH = FUND_DIR_PATH
        self._stock_info_db_file_path = '{}{}'.format(self.FUND_DIR_PATH, self._ticker_data_table_name)

        # sql connection
        self.logger.info(":: establish connection with {} ::".format(stock_info_db_file_name))
        self.data_con = sqlite3.connect(self.stock_info_db_file_path)
        self.data_cur = self.data_con.cursor()
        
        TickerDataHandler.__init__(self, self.logger, self.data_con, self.data_cur, self._stock_info_db_file_name, self._ticker_data_table_name)

    @property
    def ticker_data_table_name(self):
        return self._ticker_data_table_name
    
    @ticker_data_table_name.setter
    def ticker_data_table_name(self, table_name:str):
        self._ticker_data_table_name = table_name

    @property
    def stock_info_db_file_name(self):
        return self._stock_info_db_file_name
    
    @stock_info_db_file_name.setter
    def stock_info_db_file_name(self, db_file_name:str):
        self._stock_info_db_file_name = db_file_name
        self._stock_info_db_file_path = '{}{}'.format(self.FUND_DIR_PATH, self._stock_info_db_file_name)

    def is_stock_info_table_exists(self, table_name:str) -> bool:
        """check if the input table name exists in stock_info.db

        Args:
            table_name (str, optional): table name.

        Returns:
            bool: is table exists
        """

        self.logger.info("check {} table exist".format(stock_info_db_file_name))
        logging_info = '- \'{}\' table exist: '.format(table_name)
        
        is_exist = False
        if (table_name in self._data_table_names()):
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info)
        
        # return
        return is_exist
        
    def _data_table_names() -> list[str]:
        ...