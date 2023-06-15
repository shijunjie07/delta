from delta.database_handler.nodata import noDataDB
from delta.database_handler.ticker import tickerDB
import logging

class DBHandler(noDataDB, tickerDB):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str,
                 NO_DATA_DB_PATH:str):
        """init all db handler class
        
        Args:
            logger (logging.Logger): _description_
            DB_PATH (str): _description_
        """
        
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.NO_DATA_DB_PATH = NO_DATA_DB_PATH
        
        # init
        tickerDB.__init__(self, self.logger, self.DB_PATH)
        noDataDB.__init__(self, self.logger, self.NO_DATA_DB_PATH)