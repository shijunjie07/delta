# -----------------------
# tickerDB class
# @author: Shi Junjie
# Wed 14 Jun 2023
# -----------------------

import pandas as pd
import sqlite3
import logging



class TickerDB(GetData, LoadData):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()
        
        self.exist_ticker_table_names = self._ticker_table_names()
        self.table_types = ['eod', 'intra']
        
        GetData.__init__(self, self.logger, self.DB_PATH)
        LoadData.__init__(self, self.logger, self.DB_PATH)

    def get_mrkcap_tkls(
        self, ticker_path:str, market_caps:list[str]
    ) -> list[str]:
        """_summary_

        Args:
            ticker_path (str): _description_
            market_caps (list[str]): _description_

        Returns:
            list[str]: a list of tickers
        """
        tickers = []
        for cap in market_caps:
                    tickers.extend(list(pd.read_csv(f"{ticker_path}{cap}.csv")["Symbol"]))
        return [tkl.replace(" ", "") for tkl in tickers if (isinstance(tkl, str))]

    def is_tkl_tables_exist(self, ticker:str) -> tuple[bool, list[str]]:
        """check if the input ticker exists in database

        Args:
            ticker (str): _description_

        Returns:
            tuple[bool, list[str]]: _description_
        """
 
        logging_info = 'check if {} tables exist: '.format(ticker)
        crt_tables = []
        for table_type in self.table_types:
            table_name = '_'.join([ticker, table_type])
            if (table_name in self.exist_ticker_table_names):
                continue
            else:
                # append for later action: create table
                crt_tables.append(table_name)

        if (crt_tables):
            is_exist = False
        else:
            is_exist = True

        logging_info = logging_info + str(is_exist)
        self.logger.info(logging_info) 
        
        # return
        return is_exist, crt_tables

    def crt_tkl_tables(self, ticker:str, table_types:list[str]):
        """create ticker tables with the input table types

        Args:
            ticker (str): _description_
            table_types (str): _description_
        """
        # crt tables
        if ('eod' in table_types):
            #create eod
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_eod(\
                date_day DATE UNIQUE, d_open FLOAT, d_high FLOAT, d_low FLOAT, d_close FLOAT, d_volume BIGINT);")
            self.con.commit()
        if ('intra' in table_types):
            #create intra
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {ticker}_intra(\
                date_time DATETIME UNIQUE, m_open FLOAT, m_high FLOAT, m_low FLOAT, m_close FLOAT, m_volume BIGINT);")
            self.con.commit()

        logging_info = '- created {} tables'.format(', '.join(table_types))
        self.logger.info(logging_info)

    def _ticker_table_names(self):
        """
        
        """
        rows = self.cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        rows = self.cur.fetchall()
        return [x[0] for x in rows]

    def _format_column_names(
        self, df: pd.DataFrame, table_type:str
    ) -> tuple[bool, pd.DataFrame]:
        """Formats the column names of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame whose column names need to be formatted.

        Returns:
            tuple[bool, pd.DataFrame]: A tuple containing a boolean value indicating
            whether the formatting was successful and the DataFrame with the
            formatted column names.
        """
        self.logger.info('- start to format columns for {} push'.format(table_type))
        # Create a new DataFrame to store the formatted column names
        formatted_df = pd.DataFrame()
        
        # check table types
        if (table_type == 'eod'):
            # Mapping of original eod column names to formatted column names
            columns = {
                'date': 'date_day',
                'open': 'd_open',
                'high': 'd_high',
                'low': 'd_low',
                'close': 'd_close',
                'volume': 'd_volume',
            }
        elif (table_type == 'intra'):
            # Mapping of original intra column names to formatted column names
            columns = {
                'timestamp': 'date_time',
                'open': 'm_open',
                'high': 'm_high',
                'low': 'm_low',
                'close': 'm_close',
                'volume': 'm_volume',
            }
        else:
            self.logger.info('- fail to format, wrong table type: \'{}\''.format(table_type))
            return False, formatted_df
        
        # Check if the keys of columns match the column names of the DataFrame
        if set(columns.keys()) != set(df.columns):
            self.logger.info('- fail to format, keys of columns do not match the column names of the df')
            return False, formatted_df
        
        # Iterate over the original column names and format them
        for col in df.columns:
            formatted_df[columns[col]] = df[col]
        
        # Return the DataFrame with the formatted column names
        return True, formatted_df 


class GetData(tickerDB):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()


    def pull_tkl_dts(
        self, ticker:str, start_date:str,
        end_date:str, start_ts:int, end_ts:int
    ) -> tuple[list[str], list[int]]:
        """pull existing dates and timestamps from database
`
        Args:
            ticker (str): _description_
            start_date (str): _description_
            end_date (str): _description_
            start_ts (int): _description_
            end_ts (int): _description_

        Returns:
            tuple[list[str], list[int]]: _description_
        """
    
        dt_obj_index = 0
        
        # queries
        eod_query = 'SELECT date_day FROM {}_eod WHERE date_day>={} AND date_day<={};'.format(
            ticker, start_date.replace('-', ''), end_date.replace('-', '')
        )
        intra_query = 'SELECT date_time FROM {}_intra WHERE date_time>={} AND date_time<={};'.format(
            ticker, start_ts, end_ts
        )
        
        # pull from database
        raw_eod = self.con.execute(eod_query).fetchall()    # eod date "%Y%m%d"
        raw_intra = self.con.execute(intra_query).fetchall() # intra datetime unix timestamp
        
        # extract dates
        if (raw_eod):
            dates = [str(x[dt_obj_index]) for x in raw_eod]
            dates = [
                '{}-{}-{}'.format(x[:4], x[:6], x[6:])
                for x in dates
            ]
        else:
            dates = []
        
        # extract timestamps
        if (raw_intra):
            timestamps = [int(x[dt_obj_index]) for x in raw_intra]
        else:
            timestamps = []

        self.logger.info('existing dts: {}(date) {}(tss)'.format(len(dates), len(timestamps)))

        return dates, timestamps
    
    def pull_eod():
        ...
    
    def pull_intra():
        ...


class LoadData(tickerDB):
    
    def __init__(self, logger:logging.Logger, DB_PATH:str):
        self.logger = logger
        self.DB_PATH = DB_PATH
        self.con = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.cur = self.con.cursor()

    def push_intra(self, ticker:str, df:pd.DataFrame) -> bool:
        """
        Pushes the data from a pandas DataFrame into the corresponding intra-day table in the database.

        Args:
            ticker (str): The ticker symbol of the stock or financial instrument.
            df (pd.DataFrame): The DataFrame containing the data to be pushed.

        Returns:
            bool: True if the data is successfully pushed, False otherwise.
        """
        self.logger.info('prepare for intra push')

        # format column name
        is_success_format, df = self._format_column_names(df, 'intra')
        if (not is_success_format):
            self.logger.info('* fail to push intra')
            return False
        
        # push data
        try:
            df.to_sql('{}_intra'.format(ticker), self.con, if_exists='append', index=False)
            self.con.commit()
            self.logger.info('success push intra')
            return True
        except Exception as e:
            self.logger.info('error occurred while pushing intra, {}'.format(ticker, e))
            return False
        
    def push_eod(self, ticker: str, df: pd.DataFrame) -> bool:
        """
        Pushes the data from a pandas DataFrame into the corresponding end-of-day (EOD) table in the database.

        Args:
            ticker (str): The ticker symbol of the stock or financial instrument.
            df (pd.DataFrame): The DataFrame containing the data to be pushed.

        Returns:
            bool: True if the data is successfully pushed, False otherwise.
        """
        self.logger.info('prepare for eod push')
        # format column name
        is_success_format, df = self._format_column_names(df, 'eod')
        if (not is_success_format):
            self.logger.info('* fail to push eod')
            return False
        
        # push data
        try:
            df.to_sql('{}_eod'.format(ticker), self.con, if_exists='append', index=False)
            self.con.commit()
            self.logger.info('success push eod')
            return True
        except Exception as e:
            self.logger.info('error occurred while pushing eod, {}'.format(ticker, e))
            return False