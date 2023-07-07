# -----------------------
# DBHandler class
# @author: Shi Junjie
# Fri 15 Jun 2023
# -----------------------

from delta.sql_handler.fund import FundDB
from delta.sql_handler.nodata import NoDataDB
from delta.sql_handler.ticker import TickerDB
import logging
import os

db_file_name = 'findata.db'
nodata_db_file_name = 'nodata.db'
fund_dir_name = 'fund/'

us_exg_pickle = 'us.pickle'
ticker_data_table_name = 'tkl_data'
ticker_data_db_file_name = 'data.db'
hist_mktcap_table_name = '{}_hist_mktcap'


class DBHandler(TickerDB, NoDataDB, FundDB):
    
    def __init__(self, logger:logging.Logger, data_dir_path:str):
        
        self.logger = logger
        self.DATA_DIR_PATH = data_dir_path
        self.DB_PATH = '{}{}'.format(self.DATA_DIR_PATH, db_file_name)
        self.NO_DATA_DB_PATH = '{}{}'.format(self.DATA_DIR_PATH, nodata_db_file_name)
        self.FUND_DIR_PATH = '{}{}'.format(self.DATA_DIR_PATH, fund_dir_name)
        
        # init db files
        self._init_db_files()
        
        # init
        NoDataDB.__init__(self, self.logger, self.NO_DATA_DB_PATH)
        TickerDB.__init__(self, self.logger, self.DB_PATH)
        FundDB.__init__(self, self.logger, self.FUND_DIR_PATH)

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
        if (not os.path.isfile(self.DB_PATH)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(db_file_name, self.DATA_DIR_PATH))
            open(self.DB_PATH, 'w').close()
        
        # create nodata.db
        if (not os.path.isfile(self.NO_DATA_DB_PATH)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(nodata_db_file_name, self.DATA_DIR_PATH))
            open(self.NO_DATA_DB_PATH, 'w').close()
