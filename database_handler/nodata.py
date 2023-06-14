import os
import pandas as pd
import sqlite3
import logging
import datetime as dt



class noDataDB:
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.NO_DATA_DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.NO_DATA_DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()
        
    
    def push_no_data_dts(
        self, ticker:str, dts:list[list],
    ) -> bool:
        """push no data dts of a ticker from database

        Args:
            ticker (str): _description_
            dts (list[list]): _description_

        Returns:
            bool: _description_
        """

    def pull_no_data_dts(
        self, ticker:str,
    ) -> tuple[list]:
        """pull no data dts of a ticker from database

        Args:
            ticker (str): _description_

        Returns:
            tuple[list]: _description_
        """
        