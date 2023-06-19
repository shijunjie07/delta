# ----------------------------------------------------------
# main
# @author: Shi Junjie
# Sat 3 Jun 2023
# ----------------------------------------------------------

import os
import logging
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

# local packages
from delta.utils import Utils
from delta.sql_handler import DBHandler
from delta.request_handler import EodApiRequestHandler


# Configure the logger
date_now = dt.datetime.now().strftime("%Y-%m-%dT%H-%M")
logger_file_path = '{}/deltaLog_{}.log'.format(os.environ['LOG_PATH'], date_now)
logger = logging.basicConfig(
    filename=logger_file_path,
    level=logging.INFO, format='%(asctime)s:%(levelname)s: | %(message)s'
)
logger = logging.getLogger(__name__)


class DBUpdater(
    Utils, DBHandler,
    EodApiRequestHandler,
):
    """database update class

    Args:
        Utils (_type_): _description_
        tickerDB (_type_): _description_
        noDataDB (_type_): _description_
        eodApiRequestHandler (_type_): _description_
    """
    
    def __init__(
        self, logger:logging.Logger, activate_logger:bool=True,
        tickers:list[str]=None,
    ):
        """init DatabaseUpdate

        Args:
            logger (logging.Logger): _description_
            activate_logger (bool, optional): _description_. Defaults to True.
        """
        self.logger = logger    # logger
        self.activate_logger = activate_logger  # determine if activate logger
        if (self.activate_logger):
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARNING)

        # init env vars
        self.DB_PATH = os.environ['DB_PATH']
        self.API_KEY = os.environ['API_KEY']
        self.LOG_PATH = os.environ['LOG_PATH']
        self.NO_DATA_DB_PATH = os.environ['NO_DATA_DB_PATH']
        
        self.market_caps = [        # market caps to pull tickers
            'nano',
            'micro',
            'small',
            'medium',
            'large',
            'mega',
        ]
        
        if (not tickers):
            self.TICKER_PATH = os.environ['TICKER_PATH']
            self.tickers = self.get_mrkcap_tkls(self.TICKER_PATH, self.market_caps)   # tickers to update
        else:
            self.TICKER_PATH = None
            self.tickers = tickers

        # init classes
        Utils.__init__(self, self.logger)
        DBHandler.__init__(self, self.logger, self.DB_PATH, self.NO_DATA_DB_PATH)
        EodApiRequestHandler.__init__(self, self.API_KEY)

        self.exchange = 'us'     # exchange code

        self.error_tkls = []       # list of tickers encountered error
        self.max_days = 118     # maximum periods between ‘from’ and ‘to’ for 1 minute intra data

    # main func
    def update(self, start_date:str, end_date:str):
        """Update

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
                    len(self.tickers), ', '.join(self.market_caps), self.exchange,
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
            
            # check ticker table exists, if not create tables
            is_tkl_table_exists, crt_tkl_tables = self.is_tkl_tables_exist(ticker)
            # if ticker table not exists, then create table 
            if (not is_tkl_table_exists):
                self.crt_tkl_tables(ticker, crt_tkl_tables)        # creat tables
            # chcek nodata dt table exists, if not create table
            is_nodata_table_exists = self.is_nodata_table_exists(ticker)
            if (not is_nodata_table_exists):
                self.crt_nodata_table(ticker)
                
            # by this time, we will have all dt and ticker table types in place.
            # ----------------------------------------------------

            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(
                ticker, start_date, end_date,
                trading_timestamps[0], trading_timestamps[-1],
            )

            # pull nodata dts
            nodata_trading_dates, nodata_timestamps = self.pull_nodata_dts(ticker)
            exist_dates.append(nodata_trading_dates)
            exist_timestamps.append(nodata_timestamps) 
            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=trading_dates, reference_timestamps=trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            
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
                    self.logger.info('eod request success: {}; \'{}\', save for later action'.format(
                        is_success_eod_request, eod_json,
                    ))
                    self.error_tkls.append(ticker) # error, save for later action
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
                        self.logger.info('intra request success ({} - {}): {}; \'{}\', save for later action'.format(
                            start_ts, end_ts,
                            is_success_intra_request, intra_json_perd,
                        ))
                        self.error_tkls.append(ticker) # error, save for later action
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
            is_success_eod_push = self.push_eod(ticker, df_eod)
            is_success_intra_push = self.push_intra(ticker, df_intra)
            if ((not is_success_eod_push) or (not is_success_intra_push)):
                self.logger.info('error pushing data, save for later action')
                self.error_tkls.append(ticker)
                continue
            
            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(
                ticker, start_date, end_date,
                trading_timestamps[0], trading_timestamps[-1],
            )
            
            # double check missing
            # if still missing then define as no data dates
            # push to NODATADB
            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=trading_dates, reference_timestamps=trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            # continue if no missing dts
            if ((not missing_trading_dates) and (not missing_timestamps)):
                self.logger.info('no missing dts; moving to next ticker')
                continue
            else:
                # push no data dts
                self.push_nodata_dts(exist_dates, missing_trading_dates, missing_timestamps)
            
            # move to next ticker

        # finishing update
        update_complete_info = "Complete {} tickers update, {} fail to update.".format(len(self.tickers)-len(self.error_tkls), len(self.error_tkls))
        self.logger.info(update_complete_info)
        print(update_complete_info)
        
        # choose to save error tickers
        self._check_save_error_tkls(self.error_tkls)


# updater = DBUpdater(logger, activate_logger=True)
# updater.update('2021-01-01', '2023-02-02')