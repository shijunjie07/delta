# ----------------------------------------------------------
# main
# @author: Shi Junjie
# Sat 3 Jun 2023
# ----------------------------------------------------------

import os
import pytz
import time
import json
import logging
import numpy as npß
import pandas as pd
import datetime as dt
from tqdm import tqdm
import pandas_market_calendars as mcal

# local packages
from delta.utils import Utils
from delta.db import DB


# Configure the logger
date_now = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
logger = logging.basicConfig(
    filename='{}/deltaLog_{}.log'.format(os.environ['LOG_PATH'], date_now),
    level=logging.INFO, format='%(asctime)s:%(levelname)s: | %(message)s'
)
logger = logging.getLogger(__name__)


class databaseUpdate(Utils, DB):
    """_summary_
    """
    
    def __init__(self, logger:logging.Logger):
        self.logger = logger    # logger
        
        # init env vars
        self.DB_PATH = os.environ['DB_PATH']
        self.API_KEY = os.environ['API_KEY']
        self.LOG_PATH = os.environ['LOG_PATH']
        self.TICKER_PATH = os.environ['TICKER_PATH']
        self.NO_DATA_DB_PATH = os.environ['NO_DATA_DB_PATH']
        
        # init classes
        Utils.__init__(self, self.logger)
        DB.__init__(self, self.logger, self.DB_PATH)
        
        self.market_caps = [        # market caps to pull tickers
            'nano',
            'micro',
            'small',
            'medium',
            'large',
            'mega',
        ]
        self.exchanges = ['us']     # exchange codes
        self.tickers = self.get_mrkcap_tkls()   # tickers to update

    # main func
    def update(self, start_date:str, end_date:str):
        """_summary_

        Args:
            start_date (str): "%Y-%m-%d"
            end_date (str): "%Y-%m-%d"
        """
        
        # construct trading dts
        trading_dates, trading_timestamps = self.all_trading_dts(start_date, end_date)
        # init info
        init_string = """
            * init update database
            - start date: {}
            - end date: {}
            - trading_dates: {}
            - trading_timestamps: {}
            - ticker count: {}
            - market caps: {}
            - exchanges: {}
            -> DB_PATH: {}
            -> API_KEY: {}
            -> LOG_PATH: {}
            -> TICKER_PATH: {}
            -> NO_DATA_DB_PATH: {}
            
            -> program starts at {} <-
            """.format(
                    start_date, end_date, len(trading_dates), len(trading_timestamps),
                    len(self.tickers), ', '.join(self.market_caps), ', '.join(self.exchanges),
                    self.DB_PATH, self.API_KEY, self.LOG_PATH, self.TICKER_PATH, self.NO_DATA_DB_PATH,
                    dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
        
        print(init_string)
        self.logger.info('\n{}'.format(init_string))

        # iter
        iter_obj = tqdm(range(len(self.tickers)))
        for i in iter_obj:
            ticker = self.tickers[i]
            iter_obj.set_description('{}'.format(ticker))
            self.logger.info('-----------------------------------')
            self.logger.info('update {}'.format(ticker))
            
            # check table exists, if not create tables
            is_table_exists = self.is_tkl_tables_exist(ticker)

            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(ticker)
            self.logger.info('existing dts: {}(date) {}(tss)'.format(len(exist_dates), len(exist_timestamps)))

            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=trading_dates, reference_timestamps=trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            self.logger.info('missing dts: {}(date) {}(tss)'.format(len(missing_trading_dates), len(missing_timestamps)))
            
            # request from api
            
            # push eod & intra
            
            # pull dates & tss from db
            
            # double check missing
            # if still missing then define as no data dates
            # push to NODATADB
            
            # move to next ticker
            exit()
        
        
databaseUpdate(logger).update('2021-01-01', '2021-02-02')