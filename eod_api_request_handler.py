from eod import EodHistoricalData
import datetime as dt

class eodApiRequestHandler:
    
    def __init__(self, api_key:str):
        self.api_key = api_key
        self.api_client = EodHistoricalData(self.api_key)

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
            eod_json = self.api_client.get_prices_eod(
                '{}.{}'.format(ticker, exchange),
                period=period, order=order,
                from_=start_date, to=end_date,
            )
            return True, eod_json
        except:
            return False, 'HTTP exception raised'

    def request_intra(
        self, ticker:str, exchange:str,
        start_ts:str, end_ts:str,
        interval='1m',
    ):
        """_summary_

        Args:
            ticker (str): _description_
            exchange (str): _description_
            start_ts (str): _description_
            end_ts (str): _description_
            interval (str, optional): _description_. Defaults to '1m'.

        Returns:
            _type_: _description_
        """
        try:
            intra_json = self.api_client.get_prices_intraday(
                '{}'.format(ticker, exchange), interval=interval,
                from_=start_ts, to=end_ts
            )
            return True, intra_json
        except:
            return False, 'HTTP exception raised'
        
