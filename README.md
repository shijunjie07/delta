# DELTA

## ToDo: DBUpdater

- 
- threading for the 'DBUpdater'

## ToDo: DBHandler

- write pull_eod()
- write pull_intra()
- write pull_ipo_dates()

## ToDo: EodApiRequestHandler

- write request_tickers()
- write and implement _api_counts()
- optimise intra request to achive minimum api calls

## ToDo: Utils
- implement a function to check if start date before ipo date on db

## ToDo: Readme

- ...



-------------------------------
## A tool box for stock market sql database and market data requests from API (EODHISTORICALDATA, etc...)

Delta is a useful tool box for EODHISTORICALDATA.com users to ...

- request from api
- push and pull from database
- ✨Magic ✨

## one-liner quick start
```python
updater = DatabaseUpdate(activate_logger=True)
updater.update('2021-01-01', '2023-02-02')
```

## set env vars
For the program to run in the first place, five environment variables should be set in advance.
```
conda env config set vars DB_PATH=/path/to/your/.db                  # OHLCV database
conda env config set vars API_KEY="{YOUR API KEY}"                   # eodhistoricaldata api
conda env config set vars LOG_PATH=/path/to/your/log/dir/            # log directory
conda env config set vars TICKER_PATH=/path/to/your/ticker/dir/      # ticker directory
conda env config set vars NO_DATA_DB_PATH=/path/to/your/.db          # nodata database
```
