# DELTA

## Contents
- [TO DO](#to-do)
- [Introduction](#introduction)
- [Installation](#set-env-vars)
- [How to Use](#how-to-use)
- [...]()

## To Do
...

## Introduction
### A tool box for stock market sql database and market data requests from API (EODHISTORICALDATA, etc...)
Delta is a useful tool box for EODHISTORICALDATA.com users to ...

- request from api
- push and pull from database
- ✨Magic ✨

## set env vars
For the program to run in the first place, five environment variables should be set in advance.
```
conda env config set vars DATA_DIR_PATH=/path/to/your/data/dir       # data directory
conda env config set vars API_KEY="{YOUR API KEY}"                   # eodhistoricaldata api
conda env config set vars LOG_PATH=/path/to/your/log/dir/            # log directory
```

## How to use
### Updater
```python
updater = DatabaseUpdate(activate_logger=True)
updater.update('2000-12-07', '2001-10-14')
```
