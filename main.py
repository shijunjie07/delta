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
from eod import EodHistoricalData
import pandas_market_calendars as mcal

# local packages
from delta.utils import Utils
from delta.db import DB
from request_handler import eodApiRequestHandler



# Configure the logger
date_now = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
logger_file_path = '{}/deltaLog_{}.log'.format(os.environ['LOG_PATH'], date_now)
logger = logging.basicConfig(
    filename=logger_file_path,
    level=logging.INFO, format='%(asctime)s:%(levelname)s: | %(message)s'
)
logger = logging.getLogger(__name__)


class databaseUpdate(Utils, DB, eodApiRequestHandler):
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
        eodApiRequestHandler.__init__(self, self.API_KEY)
        
        self.market_caps = [        # market caps to pull tickers
            'nano',
            'micro',
            'small',
            'medium',
            'large',
            'mega',
        ]
        self.exchange = 'us'     # exchange code
        self.tickers = self.get_mrkcap_tkls(self.TICKER_PATH, self.market_caps)   # tickers to update
        
        self.http_error_tkls = []       # list of tickers encountered http error
        self.max_days = 118     # maximum periods between ‘from’ and ‘to’ for 1 minute intra data

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
                    len(self.tickers), ', '.join(self.market_caps), ', '.join(self.exchange),
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
            is_table_exists, crt_tables = self.is_tkl_tables_exist(ticker)
            # if table not exists, then create table 
            if (not is_table_exists):
                self.crt_tables(ticker, crt_tables)        # creat tables

            # by this time, we will have all table types in place.

            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(
                ticker, start_date, end_date,
                trading_timestamps[0], trading_timestamps[-1],
            )
            self.logger.info('existing dts: {}(date) {}(tss)'.format(len(exist_dates), len(exist_timestamps)))

            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=trading_dates, reference_timestamps=trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            self.logger.info('missing dts: {}(date) {}(tss)'.format(len(missing_trading_dates), len(missing_timestamps)))
            # continue if no missing dts
            if ((not missing_trading_dates) and (not missing_timestamps)):
                self.logger.info('no missing dts; moving to next ticker')
                continue
            
            # if missing dates
            if (missing_trading_dates):
                # request eod from api
                is_success_eod_request, eod_json = self.request_eod(
                    ticker, self.exchange, start_date,
                    end_date
                )
                # check if resp valid
                if (is_success_eod_request):
                    self.logger.info('eod request success: {}; length of data: {}'.format(
                        is_success_eod_request, len(eod_json),
                    ))
                else:
                    self.logger.info('eod request success: {}; \'{}\''.format(
                        is_success_eod_request, eod_json,
                    ))
                    self.http_error_tkls.append(ticker) # error, save for later action
                    continue
            
            # if missing timestamps
            if (missing_timestamps):
                # request intra from api
                intra_json
                # construct timestamps for periods within request start and end date
                timestamp_periods = self.timestamp_periods(
                    max_days_period=self.max_days, start_date=start_date, end_date=end_date,
                )
                is_intra_error_breaks = False
                for start_ts, end_ts in timestamp_periods:
                    # request intra from api
                    is_success_intra_request, intra_json_perd = self.request_intra(
                        ticker, self.exchange, start_ts,
                        end_ts
                    )
                    # check if resp valid
                    if (is_success_intra_request):
                        self.logger.info('intra request success ({} - {}): {}; length of data: {}'.format(
                            start_ts, end_ts,
                            is_success_intra_request, len(intra_json_perd),
                        ))
                        # extend intra_json
                        intra_json.extend(intra_json_perd)
                    else:
                        self.logger.info('intra request success ({} - {}): {}; \'{}\''.format(
                            start_ts, end_ts,
                            is_success_intra_request, intra_json_perd,
                        ))
                        self.http_error_tkls.append(ticker) # error, save for later action
                        is_intra_error_breaks = True
                        break
                # check intra http error
                if (is_intra_error_breaks):
                    intra_json = []
                    continue
            
            # construct df for eod and intra
            df_eod = pd.DataFrame(eod_json)
            df_intra = pd.DataFrame(intra_json)
            
            # filter out non missing data
            df_eod = df_eod[df_eod['date'] == missing_trading_dates].drop(
                columns=['adjusted_close'], axis=1,
            )
            df_intra = df_intra[df_intra['timestamp'] == missing_trading_dates].drop(
                columns=['gmtoffset', 'datetime'], axis=1,
            )
            
            # push eod & intra
            eod_push_status = self.push_eod(ticker, df_eod)
            intra_push_status = self.push_intra(ticker, df_intra)
            
            # pull dates & tss from db
            
            # double check missing
            # if still missing then define as no data dates
            # push to NODATADB
            
            # move to next ticker
            # exit()
        
        
databaseUpdate(logger).update('2021-01-01', '2023-02-02')