# ----------------------------------------------------------
# logger
# @author: Shi Junjie
# Mon 19 Jun 2023
# ----------------------------------------------------------

import os
import logging
import datetime as dt

# Configure the logger
date_now = dt.datetime.now().strftime("%Y-%m-%dT%H-%M")
logger_file_path = '{}/deltaLog_{}.log'.format(os.environ['LOG_PATH'], date_now)
logger = logging.basicConfig(
    filename=logger_file_path,
    level=logging.INFO, format='%(asctime)s:%(levelname)s: | %(message)s'
)
logger = logging.getLogger(__name__)
