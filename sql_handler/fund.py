# -----------------------
# FundDB class
# @author: Shi Junjie
# Thu 29 Jun 2023
# -----------------------

import os
import pickle
import logging
import sqlite3
from delta.utils import Utils

us_exg_pickle = 'us.pickle'
ticker_data_db_file_name = 'data.db'

class FundDB:

    def __init__(self, logger:logging.Logger, FUND_DIR_PATH:str):
        self.logger = logger
        self.FUND_DIR_PATH = FUND_DIR_PATH  # dir
        self.us_fund_file_path = '{}{}'.format(self.FUND_DIR_PATH, self.us_exg_pickle)
        self.ticker_data_db_file_path = '{}{}'.format(self.FUND_DIR_PATH, self.ticker_data_file_path)
        
        # sql connection
        self.logger.info(":: establish connection with data.db ::")
        self.con = sqlite3.connect(self.ticker_data_db_file_path)
        self.cur = self.con.cursor()

        # make file
        # crt us.pickle
        if (not os.path.isfile(self.us_fund_file_path)):
            self.logger.info("create pickle file in \'{}\'".format(self.FUND_DIR_PATH))
            open('{}{}'.format(self.FUND_DIR_PATH, us_exg_pickle), 'w').close()
            self.logger.info("- created file: \'{}{}\'".format(self.FUND_DIR_PATH, us_exg_pickle))
        # crt data.db
        if (not os.path.isfile(self.us_fund_file_path)):
            self.logger.info("create data db file in \'{}\'".format(self.FUND_DIR_PATH))
            open('{}{}'.format(self.FUND_DIR_PATH, us_exg_pickle), 'w').close()
            self.logger.info("- created file: \'{}{}\'".format(self.FUND_DIR_PATH, us_exg_pickle))

    def push_fund(self, data_dicts:list[dict], file_name:str='us') -> bool:
        """ush fundamentals

        Args:
            data_dicts (list[dict]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.

        Returns:
            bool: _description_
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
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

    def pull_fund(self, tickers:list[str], file_name:str='us', all:bool=False) -> tuple[bool, list[dict]]:
        """pull fundamentals

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.
            all (bool, optional): _description_. Defaults to False.

        Returns:
            tuple[bool, list[dict]]: list of ticker fundamentals dict
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
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

    def pull_ipo_dates(self, tickers:list[str], file_name:str='us') -> tuple[bool, dict[str, str]]:
        """pull ipo dates

        Args:
            tickers (list[str]): _description_
            file_name (str, optional): _description_. Defaults to 'us'.

        Returns:
            tuple[bool, dict[str, str]]: _description_
        """
        file_path = '{}{}.pickle'.format(self.FUND_DIR_PATH, file_name)
        self.logger.info("pull ipo dates from \'{}\'".format(file_path))
        
        # check valid file path
        if (not Utils.file_exists(file_path)):
            self.logger.info('- invalid \'file_path\' \'{}\''.format(file_path))
            return False, [{}]

        
        # Retrieving the data dictionaries from the pickle file
        with open(file_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)
            self.logger.info("- pulled {} data from file".format(len(loaded_data)))

        ipo_data = {}
        append_tickers = []
        # pull ticker dicts
        for data in loaded_data:
            tkl = data['General']['Code']
            if tkl in tickers:
                append_tickers.append(tkl)
                ipo_data['%s' % tkl] = str(data['General']['IPODate'])
            else:
                continue
        not_found_tickers = list(set(tickers) - set(append_tickers))

        self.logger.info("- return {} ticker ipo dates, expected {}, {} not found".format(
            len(append_tickers)), len(tickers), len(not_found_tickers)
        )
            
        return True, ipo_data