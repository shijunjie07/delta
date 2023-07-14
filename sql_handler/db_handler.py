# # -----------------------
# # DBHandler class
# # @author: Shi Junjie
# # Fri 15 Jun 2023
# # -----------------------


_stock_price_db_file_name = 'stock_price.db'     # stock price db name
_nodata_db_file_name = 'nodata.db'               # nodata db name
_fund_dir_name = 'fund/'                         # path to store fundamentals

# imports
import os
import logging

# local imports
from delta.sql_handler.fundamental import FundDB
from delta.sql_handler.nodata import NoDataDB
from delta.sql_handler.stock_price import StockPriceDB


class DBHandler(StockPriceDB, NoDataDB, FundDB):
    
    def __init__(self, logger:logging.Logger, data_dir_path:str):
        
        self.logger = logger
        self._DATA_DIR_PATH = data_dir_path
        self._stock_price_db_file_name = _stock_price_db_file_name
        self._nodata_db_file_name = _nodata_db_file_name
        self._fund_dir_name = _fund_dir_name

        self._STOCK_PRICE_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._stock_price_db_file_name)
        self._NO_DATA_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._nodata_db_file_name)
        self._FUND_DIR_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._fund_dir_name)
        
        # init db files
        self._init_db_files()
        
        # init
        NoDataDB.__init__(self, self.logger, self._NO_DATA_DB_PATH)
        StockPriceDB.__init__(self, self.logger, self._STOCK_PRICE_DB_PATH)
        FundDB.__init__(self, self.logger, self._FUND_DIR_PATH)

    # @property
    # def stock_price_db_file_name(self):
    #     return self._stock_price_db_file_name
    
    # @stock_price_db_file_name.setter
    # def stock_price_db_file_name(self, db_file_name:str):
    #     self._stock_price_db_file_name = db_file_name
    #     self._STOCK_PRICE_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._stock_price_db_file_name)


    # @property
    # def nodata_db_file_name(self):
    #     return self._nodata_db_file_name
    
    # @nodata_db_file_name.setter
    # def nodata_db_file_name(self, db_file_name:str):
    #     self._nodata_db_file_name = db_file_name
    #     self._NO_DATA_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._nodata_db_file_name)
        
        
    # @property
    # def fund_dir_name(self):
    #     return self._fund_dir_name
    
    # @fund_dir_name.setter
    # def fund_dir_name(self, db_file_name:str):
    #     self._fund_dir_name = db_file_name
    #     self._FUND_DIR_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._fund_dir_name)
        
    # @property
    # def data_dir_path(self):
    #     return self._DATA_DIR_PATH

    # @data_dir_path.setter
    # def data_dir_path(self, dir_path:str):
    #     self._DATA_DIR_PATH = dir_path
    #     self._STOCK_PRICE_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._stock_price_db_file_name)
    #     self._NO_DATA_DB_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._nodata_db_file_name)
    #     self._FUND_DIR_PATH = '{}{}'.format(self._DATA_DIR_PATH, self._fund_dir_name)
        

    def close_all_conn(self):
        """close all db connections
        """
        self.logger.info(":: Closing All Database Connections ::")
        self.con.close()
        self.nodata_con.close()
        self.data_con.close()
        self.logger.info(":: Connection Closed ::")
        
    def _init_db_files(self):
        """init db files
        """
        self.logger.info("init db files")
        
        # create db dir
        if (not os.path.isdir(self.DATA_DIR_PATH)):
            self.logger.info("- create dir \'{}\'".format(self.DATA_DIR_PATH))
            os.mkdir(self.DATA_DIR_PATH)
        
        # create findata.db
        if (not os.path.isfile(self.STOCK_PRICE_DB_PATH)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(self._stock_price_db_file_name, self.DATA_DIR_PATH))
            open(self.STOCK_PRICE_DB_PATH, 'w').close()
        
        # create nodata.db
        if (not os.path.isfile(self.NO_DATA_DB_PATH)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(self._nodata_db_file_name, self.DATA_DIR_PATH))
            open(self.NO_DATA_DB_PATH, 'w').close()