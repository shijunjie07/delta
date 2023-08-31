# ----------------------------------------------------------
# setup
# @author: Shi Junjie
# Thu 31 Aug 2023
# ----------------------------------------------------------

from setuptools import setup, find_packages

setup(
    name='delta',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'eod',
        'tqdm',
        'numpy',
        'openpyxl',
        'pandas',
        'pandas-market-calendars',
        'requests'
    ],
    author='Shi Junjie',
    author_email='shijunja@yahoo.com',
    license='MIT License',
    
    description="""tool box for EODHISTORICALDATA.com users to ...
                    - request from api
                    - push and pull from database
                    - ✨Magic ✨
                    """,
    url='https://github.com/shijunjie07/delta',
)