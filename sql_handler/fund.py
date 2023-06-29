# -----------------------
# FundDB class
# @author: Shi Junjie
# Thu 29 Jun 2023
# -----------------------

import pickle
import logging
from delta.utils import Utils

us_exg_pickle = 'us.pickle'

class FundDB:

    def __init__(self, logger:logging.Logger, FUND_PATH:str):
        self.logger = logger
        self.FUND_PATH = FUND_PATH  # dir
        
        # make file
        self.logger.info("create pickle file in \'{}\'".format(self.FUND_PATH))
        open('{}{}'.format(self.FUND_PATH, us_exg_pickle), 'w').close()
        self.logger.info("- created file: \'{}{}\'".format(self.FUND_PATH, us_exg_pickle))

    def push_fund(self, file_name:str, data_dicts:list[dict]) -> bool:
        """push fundamentals

        Args:
            data_dicts (list[dict]): _description_

        Returns:
            bool: _description_
        """
        file_path = '{}{}.pickle'.format(self.FUND_PATH, file_name)
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

    def pull_fund(self, tickers:list[str], file_name:str, all:bool=False) -> tuple[bool, list[dict]]:
        """pull fundamentas

        Args:
            tickers (list[str]): _description_
            file_name (str): _description_
            all (bool, optional): _description_. Defaults to False.

        Returns:
            tuple[bool, list[dict]]: list of ticker fundamentals dict
        """
        file_path = '{}{}.pickle'.format(self.FUND_PATH, file_name)
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
            # pull necessary ticker dicts
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

    def pull_ipo_dates(self, tickers:list[str], file_name:str) -> tuple[bool, dict[str, str]]:
        """pull ipo dates

        Args:
            tickers (list[str]): _description_
            file_name (str): _description_

        Returns:
            tuple[bool, dict[str, str]]: dict[ticker, ipo_date]
        """
        # data['General']['IPODate']
        ...