# ----------------------------------------------------------
# db updater
# @author: Shi Junjie
# Sat 3 Jun 2023
# ----------------------------------------------------------

import os
import logging
import pandas as pd
import datetime as dt
from tqdm import tqdm

# local packages
from delta.utils import Utils
from delta.logger import logger
from delta.sql_handler import DBHandler
from delta.request_handler import EodApiRequestHandler
from delta.sql_handler import hist_mktcap_table_name

class DBUpdater(
    DBHandler,
    Utils,
    EodApiRequestHandler,
):
    """database update class

    Args:
        DBHandler (_type_): _description_
        Utils (_type_): _description_
        EodApiRequestHandler (_type_): _description_
    """ 
    def __init__(self, activate_logger:bool=True):
        """init DatabaseUpdate

        Args:
            activate_logger (bool, optional): _description_. Defaults to True.
            tickers (list[str], optional): _description_. Defaults to None.
        """
        self.activate_logger = activate_logger  # determine if activate logger
        if (self.activate_logger):
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.NOTSET)

        # init env vars
        self.DATA_DIR_PATH = os.environ['DATA_DIR_PATH']    # directiory
        self.API_KEY = os.environ['API_KEY']                # api key
        self.LOG_PATH = os.environ['LOG_PATH']              # log path
        self.NASDAQ_CSV = '/home/junja/findata/data/market_caps/nasdaq_screener_1690263635982_mktcap.csv'   # nasdaq symbols csv file

        self.market_caps = [        # market caps to pull tickers
            'NANO',
            'MICRO',
            'SMALL',
            'MEDIUM',
            'LARGE',
            'MEGA',
        ]
        

        # init classes
        Utils.__init__(self, logger)
        DBHandler.__init__(self, logger, self.DATA_DIR_PATH)
        EodApiRequestHandler.__init__(self, self.API_KEY)

        self.exchange = 'us'     # exchange code

        self.error_tkls = []       # list of tickers encountered error
        self.max_days = 118     # maximum periods between ‘from’ and ‘to’ for 1 minute intra data

    # main func
    def update(
        self, start_date:str, end_date:str, 
        tickers:list[str]=None
    ):
        # define tickers to update
        if tickers:
            self.tickers = tickers
        else:
            self.tickers = pd.read_csv(self.NASDAQ_CSV)['Symbol'].to_list()
        # validate ticker
        self.tickers, invalid_tickers, tkl_log_msg = Utils.validate_tickers(self.tickers)

        # construct trading dts
        trading_dates, trading_timestamps = self.all_trading_dts(start_date, end_date)
        # ipo dates
        is_success_ipo_dates, ipo_dates = self.pull_ipo_dates_from_fud(self.tickers)
        if (is_success_ipo_dates):
            if (len(ipo_dates) != len(self.tickers)):
                self.logger.info("Unmatch len {} of \'ipo_dates\' and {} of \'tickers\'".format(len(ipo_dates), len(self.tickers)))
                raise ValueError("Unmatch len {} of \'ipo_dates\' and {} of \'tickers\'".format(len(ipo_dates), len(self.tickers)))
        else:
            raise Exception("fail to pull \'ipo_dates\'")

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
            -> DATA_DIR_PATH: {}
            -> LOG_PATH: {}
            -> API_KEY: {}

            -> program starts at {} <-
            """.format(
                    start_date, end_date, len(trading_dates),
                    len(trading_timestamps), len(self.tickers), ', '.join(self.market_caps),
                    self.exchange, self.DATA_DIR_PATH, self.LOG_PATH, self.API_KEY, 
                    dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
        
        print(init_string)
        logger.info('\n{}'.format(init_string))
        
        # iter
        iter_obj = tqdm(range(len(self.tickers)))
        for i in iter_obj:
            ticker = self.tickers[i]
            iter_obj.set_description('{}'.format(ticker))
            logger.info('-------------------------------------------')
            logger.info('update {}'.format(ticker))
            
            # check ticker table exists, if not create tables
            is_tkl_table_exists, tkl_tables = self.is_stock_price_tables_exist(ticker)
            # if ticker table not exists, then create table 
            if (not is_tkl_table_exists):
                self.crt_stock_price_tables(ticker, tkl_tables)

            # check nodata dt table exists, if not create table
            is_nodata_table_exists, nodata_tables = self.is_nodata_table_exists(ticker)
            if (not is_nodata_table_exists):
                self.crt_nodata_tables(ticker, nodata_tables)
                
            # check hist market cap table exists
            is_hist_mktcap_table_exists = self.is_data_table_exists(hist_mktcap_table_name.format(ticker))
            if (not is_hist_mktcap_table_exists):
                self.crt_ticker_hist_mktcap_table(ticker)
                
            # by this time, we will have all dt and ticker table types in place.
            # ----------------------------------------------------

            # check ticker start date
            logger.info("check ticker start date")
            diff = dt.datetime.strptime(ipo_dates[ticker], '%Y-%m-%d')\
                   - dt.datetime.strptime(start_date, '%Y-%m-%d')
            if (diff.days > 0):
                tkl_start_date = ipo_dates[ticker]
                # update trading dts
                if (tkl_start_date in trading_dates):
                    tkl_trading_dates = trading_dates[trading_dates.index(tkl_start_date):]
                    start_ts = self._construct_trading_period_timestamps(tkl_start_date, tkl_start_date)[0]
                    tkl_trading_timestamps = trading_timestamps[trading_timestamps.index(start_ts):]
                    logger.info("- adjust ticker start date({}) to its ipo date({})".format(start_date, ipo_dates[ticker]))
                    logger.info("- adjust trading dts: {}(dates) {}(tss)".format(len(tkl_trading_dates), len(tkl_trading_timestamps)))
                else:
                    logger.info("- update period before its ipo date")
                    continue
            else:
                logger.info("- no change on start date")
                tkl_start_date = start_date
            
            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(
                ticker, tkl_start_date, end_date,
                tkl_trading_timestamps[0], tkl_trading_timestamps[-1],
            )


            # pull nodata dts
            nodata_trading_dates, nodata_timestamps = self.pull_nodata_dts(ticker)
            exist_dates.append(nodata_trading_dates)
            exist_timestamps.append(nodata_timestamps) 
            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=tkl_trading_dates, reference_timestamps=tkl_trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            
            if ((not missing_trading_dates) and (not missing_timestamps)):
                logger.info('no missing dts; moving to next ticker')
                continue
            
            # if missing dates
            if (missing_trading_dates):
                # request eod from api
                is_success_eod_request, eod_json = self.request_eod(
                    ticker, self.exchange, tkl_start_date,
                    end_date
                )
                # check if resp valid
                if (is_success_eod_request):
                    logger.info('eod request success: {}; length of data: {}'.format(
                        is_success_eod_request, len(eod_json),
                    ))
                else:
                    logger.info('eod request success: {}; \'{}\', save for later action'.format(
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
                    max_days_period=self.max_days, start_date=tkl_start_date, end_date=end_date,
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
                        logger.info('intra request success ({} - {}): {}; length of data: {}'.format(
                            start_ts, end_ts,
                            is_success_intra_request, len(intra_json_perd),
                        ))
                        # extend intra_json
                        intra_json.extend(intra_json_perd)
                    else:
                        logger.info('intra request success ({} - {}): {}; \'{}\', save for later action'.format(
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
            
            # filter out non-missing dts and un-want columns
            df_eod = df_eod[df_eod['date'] == missing_trading_dates]
            df_intra = df_intra[df_intra['timestamp'] == missing_trading_dates]
            
            # push eod & intra
            is_success_eod_push = self.push_eod(ticker, df_eod)
            is_success_intra_push = self.push_intra(ticker, df_intra)
            if ((not is_success_eod_push) or (not is_success_intra_push)):
                logger.info('error pushing data, save for later action')
                self.error_tkls.append(ticker)
                continue
            
            # pull dates & tss from db
            exist_dates, exist_timestamps = self.pull_tkl_dts(
                ticker, tkl_start_date, end_date,
                tkl_trading_timestamps[0], tkl_trading_timestamps[-1],
            )
            
            # double check missing
            # if still missing then define as no data dates
            # push to NODATADB
            # check missing
            missing_trading_dates, missing_timestamps = self.missing_dts(
                reference_dates=tkl_trading_dates, reference_timestamps=tkl_trading_timestamps,
                comparant_dates=exist_dates, comparant_timestamps=exist_timestamps
            )
            # continue if no missing dts
            if ((not missing_trading_dates) and (not missing_timestamps)):
                logger.info('no missing dts; moving to next ticker')
                continue
            else:
                # push no data dts
                self.push_nodata_dts(exist_dates, missing_trading_dates, missing_timestamps)
            
            # move to next ticker

        # finishing update
        update_complete_info = "Complete {} tickers update, {} fail to update.".format(len(self.tickers)-len(self.error_tkls), len(self.error_tkls))
        logger.info(update_complete_info)
        print(update_complete_info)
        
        # choose to save error tickers
        self._check_save_error_tkls(self.error_tkls)
        