# -----------------------
# DBHandler class
# @author: Shi Junjie
# Fri 15 Jun 2023
# -----------------------

from delta.sql_handler.nodata import NoDataDB
from delta.sql_handler.ticker import TickerDB
import logging

class DBHandler(TickerDB, NoDataDB):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str,
                 NO_DATA_DB_PATH:str):
        """DBHandler class
        
        Args:
            logger (logging.Logger): _description_
            DB_PATH (str): _description_
        """
        
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.NO_DATA_DB_PATH = NO_DATA_DB_PATH
        
        # init
        TickerDB.__init__(self, self.logger, self.DB_PATH, self.NO_DATA_DB_PATH)
        NoDataDB.__init__(self, self.logger, self.NO_DATA_DB_PATH)