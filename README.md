# DELTA

## Contents
- [TO DO LIST](#to-do-list)
- [Introduction](#introduction)
- [Installation](#set-env-vars)
- [How to Use](#how-to-use)
- [...]()

# To DO LIST
## ToDo: DBUpdater
 
- threading for the 'DBUpdater'
- implement store ipo dates - refer DBHandler data.db
-

## ToDo: DBHandler

- write pull_eod()
- write pull_intra()
- write pull_ipo_dates()
- store ipo dates for all tickers (data.db)
-

## ToDo: EodApiRequestHandler

- write request_tickers()
- write and implement _api_counts()
- optimise intra request to achive minimum api calls
- if data return is empty because of no data at that time for the reuqest ticker - HOW TO HANDLER EXCEPTION - WRITE TEST
- 

## ToDo: Utils
- implement a function to check if start date before ipo date on db
-

## ToDo: Readme

- mention ESTERN timezone
-

# Introduction
-------------------------------
## A tool box for stock market sql database and market data requests from API (EODHISTORICALDATA, etc...)

Delta is a useful tool box for EODHISTORICALDATA.com users to ...

- request from api
- push and pull from database
- ✨Magic ✨

## set env vars
For the program to run in the first place, five environment variables should be set in advance.
```
conda env config set vars DB_PATH=/path/to/your/.db                  # OHLCV database
conda env config set vars NO_DATA_DB_PATH=/path/to/your/.db          # nodata database
conda env config set vars API_KEY="{YOUR API KEY}"                   # eodhistoricaldata api
conda env config set vars LOG_PATH=/path/to/your/log/dir/            # log directory
conda env config set vars TICKER_PATH=/path/to/your/ticker/dir/      # ticker directory
conda env config set vars FUND_PAT=/path/to/your/fundamentals/dir/   # fundamentals directory
```

# How to use
## Updater
```python
updater = DatabaseUpdate(activate_logger=True)
updater.update('2000-12-07', '2001-10-14')
```
