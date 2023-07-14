# -----------------------
# FundDB class
# @author: Shi Junjie
# Thu 29 Jun 2023
# -----------------------

import os
import pickle
import logging

from delta.utils import Utils
from delta.sql_handler.fundamental.stock_info.stock_info import StockInfoDB

_us_exg_pickle = 'us.pickle'                     # file to store fundamentals



class FundDB(StockInfoDB):

    def __init__(self, logger:logging.Logger, _FUND_DIR_PATH:str):
        self.logger = logger
        self._FUND_DIR_PATH = _FUND_DIR_PATH  # dir
        self._us_exg_pickle = _us_exg_pickle
        self._us_fund_file_path = '{}{}'.format(self._FUND_DIR_PATH, self._us_exg_pickle)
        
        # init fund files
        self._init_fund_files()
        
        # init data.db
        StockInfoDB.__init__(self, self.logger, self._FUND_DIR_PATH)


    def _init_fund_files(self):
        """init fund files
        """
        self.logger.info("init fund files")
        # create fundamental dir
        if (not os.path.isdir(self._FUND_DIR_PATH)):
            self.logger.info("- create dir \'{}\'".format(self._FUND_DIR_PATH))
            os.mkdir(self._FUND_DIR_PATH)
            
        # crt us.pickle
        if (not os.path.isfile(self._us_fund_file_path)):
            self.logger.info("- create \'{}\' file in \'{}\'".format(self._us_exg_pickle, self._FUND_DIR_PATH))
            open(self._us_fund_file_path, 'w').close()
            
        # crt stock_info.db
        if (not os.path.isfile(self._stock_info_db_file_path)):
            self.logger.info("- create data \'{}\' file in \'{}\'".format(self._stock_info_db_file_path, self._FUND_DIR_PATH))
            open(self._stock_info_db_file_path, 'w').close()

    
    def push_fund(self, data_dicts:list[dict]) -> bool:
        """ush fundamentals

        Args:
            data_dicts (list[dict]): _description_
            self._us_exg_pickle (str, optional): _description_. Defaults to self._us_exg_pickle.

        Returns:
            bool: _description_
        """
        file_path = '{}{}'.format(self._FUND_DIR_PATH, self._us_exg_pickle)
        self.logger.info("push fundamentals to \'{}\'".format(file_path))

        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
        
        # check valid data dict
        if (len(data_dicts) == 0):
            self.logger.info("- invalid \'data_dicts\', length: 0")
            return False

        # make unique 
        unique_data_dicts = list(set(data_dicts))
        self.logger.info("input: {} dicts, unique: {} dicts".format(
            len(data_dicts), len(unique_data_dicts)
        ))

        # append new data
        with open(file_path, 'ab') as pickle_file:
            pickle.dump(unique_data_dicts, pickle_file)

        self.logger.info("- success push {} dicts".format(len(unique_data_dicts)))
        return True

    def pull_fund(self, tickers:list[str], all:bool=False) -> tuple[bool, list[dict]]:
        """pull fundamentals

        Args:
            tickers (list[str]): _description_
            all (bool, optional): _description_. Defaults to False.

        Returns:
            tuple[bool, list[dict]]: list of ticker fundamentals dict
        """
        file_path = '{}{}'.format(self._FUND_DIR_PATH, self._us_exg_pickle)
        self.logger.info("pull fundamentals from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))
    
        if (all):
            self.logger.info("- return all")
            return True, loaded_data
        else:
            data_dicts = []
            append_tickers = []
            # pull ticker dicts
            for data in loaded_data:
                tkl = data['General']['Code']
                if tkl in tickers:
                    append_tickers.append(tkl)
                    data_dicts.append(data)
                else:
                    continue
            not_found_tickers = list(set(tickers) - set(append_tickers))

            self.logger.info("- return {} ticker dicts, expected {}, {} not found".format(
                len(data_dicts)), len(tickers), len(not_found_tickers)
            )
            
            return True, data_dicts

    def pull_ipo_dates_from_fud(self, tickers:list[str]) -> tuple[bool, dict[str, str]]:
        """pull ipo dates from fundamentals

        Args:
            tickers (list[str]): _description_

        Returns:
            tuple[bool, dict[str, str]]: _description_
        """
        file_path = '{}{}'.format(self._FUND_DIR_PATH, self._us_exg_pickle)
        self.logger.info("pull ipo dates from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))

        ipo_date = {}
        append_tickers = []
        # pull ticker dicts
        for data in loaded_data:
            tkl = data['General']['Code']
            if tkl in tickers:
                append_tickers.append(tkl)
                ipo_date['%s' % tkl] = str(data['General']['IPODate'])
            else:
                continue
        not_found_tickers = list(set(tickers) - set(append_tickers))

        self.logger.info("- return {} ticker ipo dates, expected {}, {} not found".format(
            len(append_tickers)), len(tickers), len(not_found_tickers)
        )
            
        return True, ipo_date