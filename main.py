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
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
import pandas_market_calendars as mcal

# local packages
import eod_api
from data2base.sqlite_handler import SqliteHandler
from data2base.load_data import LoadEod, LoadIntra
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
        super().__init__(self.logger)
        self.DB_PATH = os.environ['DB_PATH']
        self.API_KEY = os.environ['API_KEY']
        self.LOG_PATH = os.environ['LOG_PATH']
        self.TICKER_PATH = os.environ['TICKER_PATH']
        self.NO_DATA_DB_PATH = os.environ['NO_DATA_DB_PATH']
        
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
        
        init_string = """
            * init update database
            - start date: {}
            - end date: {}
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
                    start_date, end_date,
                    len(self.tickers), ', '.join(self.market_caps), ', '.join(self.exchanges),
                    self.DB_PATH, self.API_KEY, self.LOG_PATH, self.TICKER_PATH, self.NO_DATA_DB_PATH,
                    dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
        
        print(init_string)
        self.logger.info('\n{}'.format(init_string))
        
        iter_obj = tqdm(range(len(self.tickers)))
        for i in iter_obj:
            ticker = self.tickers[i]
            iter_obj.set_description('{}'.format(ticker))
            self.logger.info('-----------------------------------')
            self.logger.info('update {}'.format(ticker))
            
            # check if ticker tables exists
            # if yes proceedß
            # else crt tables and request all data from api
            self.is_tkl_tables_exist(ticker)
            
            # pull dates & tss from db
            
            # check missing
            
            # request from api
            
            # push eod & intra
            
            # pull dates & tss from db
            
            # double check missing
            # if still missing then define as no data dates
            # push to NODATADB
            
            # move to next ticker
        
        
databaseUpdate(logger).update('2021-01-01', '2021-02-02')