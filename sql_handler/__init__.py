# -----------------------
# DBHandler class
# @author: Shi Junjie
# Fri 15 Jun 2023
# -----------------------

from delta.sql_handler.fund import FundDB
from delta.sql_handler.nodata import NoDataDB
from delta.sql_handler.ticker import TickerDB
import logging

db_file_name = 'findata.db'
nodata_db_file_name = 'nodata.db'
fund_dir_name = 'fund/'


class DBHandler(TickerDB, NoDataDB, FundDB):
    
    def __init__(self, logger:logging.Logger, data_dir_path:str):
        
        self.logger = logger
        self.DB_PATH = '{}{}'.format(data_dir_path, db_file_name)
        self.NO_DATA_DB_PATH = '{}{}'.format(data_dir_path, nodata_db_file_name)
        self.FUND_DIR_PATH = '{}{}'.format(data_dir_path, fund_dir_name)

        # init
        NoDataDB.__init__(self, self.logger, self.NO_DATA_DB_PATH)
        TickerDB.__init__(self, self.logger, self.DB_PATH)
        FundDB.__init__(self, self.logger, self.FUND_DIR_PATH)
    
    def close_all_conn(self):
        self.logger.info(":: Closing All Database Connections ::")
        self.con.close()
        self.nodata_con.close()
        self.data_con.close()
        self.logger.info(":: Connection Closed ::")