# -----------------------
# EodApiRequestHandler class
# @author: Shi Junjie
# Sat 3 Jun 2023
# -----------------------

import logging
import pandas as pd
from eod import EodHistoricalData
from urllib3.exceptions import HTTPError

class EodApiRequestHandler:
    
    def __init__(self, logger:logging.Logger, api_key:str, call_limit=100_000):
        """_summary_

        Args:
            api_key (str): _description_
            api_calls (int, optional): _description_. Defaults to 100000.
        """
        self.logger = logger
        self.api_key = api_key
        self.logger.info(":: init EOD API connection ::")
        self.api_client = EodHistoricalData(self.api_key)
        
        # api calls per request
        self.eod_calls_per_reqeust = 1      # end-of-day
        self.intra_calls_per_reqeust = 5    # intraday
        self.fund_calls_per_rquest = 10     # fundamental data

        # daily api count
        self.call_limit = call_limit
        self.call_used = self._call_counts()

    def request_eod(
        self, ticker:str, exchange:str, 
        start_date:str, end_date:str,
        period='d', order='a',
    ) -> tuple[bool, list]:
        """request eod data

        Args:
            ticker (str): _description_
            exchange (str): _description_
            start_date (str): _description_
            end_date (str): _description_
            period (str, optional): _description_. Defaults to 'd'.
            order (str, optional): _description_. Defaults to 'a'.

        Returns:
            tuple[bool, list]: _description_
        """
        self.logger.info("fetching eod data between {} and {}".format(start_date, end_date))
        self.call_used += self.eod_calls_per_reqeust
        try:
            eod_json = []
            eod_json = self.api_client.get_prices_eod(
                '{}.{}'.format(ticker, exchange),
                period=period, order=order,
                from_=start_date, to=end_date,
            )
            if (not eod_json):
                self.logger.info("- empty return")
                return False, ['empty return']     # ??? NOTED
            self.logger.info("- {} data point return".format(len(eod_json)))
            return True, eod_json
        except(HTTPError):
            self.logger.info("- \'HTTP exception raised\'")
            return False, ['HTTP exception raised']

    def request_intra(
        self, ticker:str, exchange:str,
        start_ts:int, end_ts:int,
        interval='1m',
    ) -> tuple[bool, list]:
        """request intraday data

        Args:
            ticker (str): _description_
            exchange (str): _description_
            start_ts (int): _description_
            end_ts (int): _description_
            interval (str, optional): _description_. Defaults to '1m'.

        Returns:
            tuple[bool, list]: _description_
        """
        self.logger.info("fetching intra data between {} and {}".format(start_ts, end_ts))
        self.call_used += self.intra_calls_per_reqeust
        try:
            intra_json = self.api_client.get_prices_intraday(
                '{}.{}'.format(ticker, exchange), interval=interval,
                from_=start_ts, to=end_ts
            )
            if (not intra_json):
                self.logger.info("- empty return")
                return False, ['empty return']     # ???
            self.logger.info("- {} data point return".format(len(intra_json)))
            return True, intra_json
        except:
            self.logger.info("- \'HTTP exception raised\'")
            return False, ['HTTP exception raised']

    def request_fundamentals(self, ticker:str, exchange:str) -> tuple[bool, dict]:
        """request fundamental data
        

        Args:
            ticker (str): _description_
            exchange (str): exhchange code

        Returns:
            tuple[bool, dict]: return dict
        """
        symbol = "{}.{}".format(ticker.upper(), exchange.upper())
        self.logger.info("fetching fundamentals of {}".format(symbol))
        self.call_used += self.fund_calls_per_rquest
        try:
            fund = self.api_client.get_fundamental_equity(symbol)
            if (not fund):
                self.logger.info("- empty return")
                return False, {'empty return': ''}
            self.logger.info("- request success")
            return True, fund
        except:
            self.logger.info("- \'HTTP exception raised\'")
            return False, {'HTTP exception raised': ''}

    def request_tickers(self, exchange:str) -> tuple[bool, dict]:
        """request exchange traded tickers

        Args:
            exchange (str): exchange code

        Returns:
            tuple[bool, dict]: exchange tickers wt info
        """
        exchange = exchange.upper()
        self.logger.info("fetching {} exchange tickers".format(exchange))
        self.call_used += self.fund_calls_per_rquest
        try:
            exg_symbols = self.api_client.get_exchange_symbols(exchange)
            if (not exg_symbols):
                self.logger.info("- empty return")
                return False, {'empty return': ''}
            self.logger.info("- request success")
            self.logger.info("- parse exchange symbols to dict")
            exg_symbols = pd.DataFrame(exg_symbols).to_dict("list")
            return True, exg_symbols
        except:
            self.logger.info("- \'HTTP exception raised\'")
            return False, {'HTTP exception raised': ''}
        
    # ?????
    def _call_counts(self) -> int:
        """request call count from eodhistoricaldata.com api

        Returns:
            int: _description_
        """
        return int(self.api_client.get_user()['apiRequests'])