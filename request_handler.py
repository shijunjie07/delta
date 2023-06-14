# -----------------------
# eodApiRequestHandler class
# @author: Shi Junjie
# Sat 3 Jun 2023
# -----------------------

import datetime as dt
from eod import EodHistoricalData
from urllib3.exceptions import HTTPError

class eodApiRequestHandler:
    
    def __init__(self, api_key:str, api_calls=100000):
        """_summary_

        Args:
            api_key (str): _description_
            api_calls (int, optional): _description_. Defaults to 100000.
        """
        self.api_key = api_key
        self.api_client = EodHistoricalData(self.api_key)
        
        # daily api count
        self.api_calls = api_calls  # daily calls
        # api calls per request
        self.eod_calls_per_reqeust = 1      # end-of-day
        self.intra_calls_per_reqeust = 5    # intraday
        self.fund_calls_per_rquest = 10     # fundamental data

    def request_eod(
        self, ticker:str, exchange:str, 
        start_date:str, end_date:str,
        period='d', order='a',
    ):
        """_summary_

        Args:
            ticker (str): _description_
            exchange (str): _description_
            start_date (str): _description_
            end_date (str): _description_
            period (str, optional): _description_. Defaults to 'd'.
            order (str, optional): _description_. Defaults to 'a'.

        Returns:
            _type_: _description_
        """
        try:
            eod_json = []
            eod_json = self.api_client.get_prices_eod(
                '{}.{}'.format(ticker, exchange),
                period=period, order=order,
                from_=start_date, to=end_date,
            )
            if (not eod_json):
                return False, ['HTTP exception raised']     # ???
            return True, eod_json
        except(HTTPError):
            return False, ['HTTP exception raised']

    def request_intra(
        self, ticker:str, exchange:str,
        start_ts:int, end_ts:int,
        interval='1m',
    ):
        """_summary_

        Args:
            ticker (str): _description_
            exchange (str): _description_
            start_ts (int): _description_
            end_ts (int): _description_
            interval (str, optional): _description_. Defaults to '1m'.

        Returns:
            _type_: _description_
        """
        try:
            intra_json = self.api_client.get_prices_intraday(
                '{}'.format(ticker, exchange), interval=interval,
                from_=start_ts, to=end_ts
            )
            if (not intra_json):
                return False, ['HTTP exception raised']     # ???
            return True, intra_json
        except:
            return False, ['HTTP exception raised']
        