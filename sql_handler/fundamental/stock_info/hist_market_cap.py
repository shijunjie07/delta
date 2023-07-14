# -----------------------
# HistMarketCapHandler class
# @author: Shi Junjie
# Thu 13 Jul 2023
# -----------------------

import logging
import sqlite3
import pandas as pd

# table names of stock_info.db
hist_mktcap_table_name = '{}_hist_mktcap'       # historical market capitalisation of a ticker

# market cap catagory
market_cap_ctgs = {
    "NANO": 0,
    "MICRO": 50_000_000,
    "SMALL": 300_000_000,
    "MEDIUM": 2_000_000_000,
    "LARGE": 10_000_000_000,
    "MEGA": 200_000_000_000,
}


class HistMarketCapHandler:
    # update market cap to 'tkl_data' table
    # push/pull market cap to '{ticker}_hist_mktcap' tables
    # crt ticker historical market cap table
    
    def __init__(self, logger:logging.Logger, data_con:sqlite3.Connection, data_cur:sqlite3.Cursor, db_file_name:str):
        self.logger = logger
        self.data_con = data_con
        self.data_cur = data_cur
        self._stock_info_db_file_name = db_file_name

    def crt_ticker_hist_mktcap_table(self, ticker:str):
        table_name = hist_mktcap_table_name.format(ticker)
        
    
    def pull_hist_mktcap(self, ticker:str, keys:list[str]=None) -> tuple[bool, dict]:
        table_name = hist_mktcap_table_name.format(ticker)
        self.logger.info("create \'{}\' table on \'{}\'".format(table_name, self._stock_info_db_file_name))
        try:
            crt_table_query = "CREATE TABLE IF NOT EXISTS {}(trade_date DATETIME UNIQUE, mkt_cap_value BIGINT, mkt_cap TEXT);".format(table_name)
            self.data_cur.execute(crt_table_query)
            self.data_con.commit()
        except Exception as e:
            self.logger.info("- An exception occured while creating \'{}\' table: \'{}\', continue...".format(table_name, e))

        self.logger.info("- created \'{}\' table".format(table_name))    
    
    def push_hist_mktcap(self, ticker:str, df:pd.DataFrame) -> bool:
        table_name = hist_mktcap_table_name.format(ticker)
        ...
    
    def update_mktcap_2_tkldata(self) -> bool:
        ...

    def _cat_mktcap(value:int) -> str:
        """catagorise market cap

        Args:
            value (int): market cap values

        Raises:
            ValueError: _description_

        Returns:
            str: market cap
        """

        # check valid value
        positive_inf = float("inf")
        if not ((value >= 0) and (value < positive_inf)):
            raise ValueError("\'value\' should be a positive number, not \'{}\'".format(value))
        
        # find market cap
        for i in range(len(market_cap_ctgs.keys())):
            # determine threshold values
            lower_threshold_val = market_cap_ctgs[list(market_cap_ctgs.keys())[i]]

            # last one
            if (i == (len(market_cap_ctgs.keys())-1)):
                upper_threshold_val = positive_inf
            else:
                upper_threshold_val = market_cap_ctgs[list(market_cap_ctgs.keys())[i+1]]

            if ((value >= lower_threshold_val) and (value < upper_threshold_val)):
                return list(market_cap_ctgs.keys())[i]