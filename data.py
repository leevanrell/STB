#!/usr/bin/python3
from time import gmtime, strftime, sleep
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
import time
import datetime
import os
import logging
import asyncio
import sqlite3

from lib.GenericRSSData import RSS
from lib.StockData import Stock
from lib.DataScreen import Screen

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')


fh = logging.FileHandler('./data/RSS.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
logger.addHandler(fh)

#ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
#ch.setFormatter(fmt)
#logger.addHandler(ch)

Alpha_api_key = '2RPX5G5M7XOXMDJU'
Alpha_url = 'http://www.alphavantage.co/query'

Ticker_file = './data/ticker.txt'
RSS_DB_file = './data/data.db'
Stock_DB_file = './data/stock.db'

Processes_Count = 4
Fin_TIMEOUT = 5
VERSION = '0.2'


def main():
	Companies = [line.strip() for line in open(Ticker_file).readlines()]
	logger.debug('Looking up RSS on %s' % str(Companies))

	loop = asyncio.get_event_loop()
	executor = ProcessPoolExecutor()

	Yahoo = RSS(logger, RSS_DB_file, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Companies)
	NASDAQ = RSS(logger, RSS_DB_file, 'NASDAQ', 'http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=', Companies)
	DataStock = Stock(logger, Stock_DB_file, Alpha_url, Alpha_api_key, Companies)
	DataScreen = Screen(VERSION, Yahoo, NASDAQ, DataStock)

	future_Yahoo = loop.run_in_executor(None, Yahoo.run, loop)
	future_NASDAQ = loop.run_in_executor(None, NASDAQ.run, loop)
	future_Stock = loop.run_in_executor(None, DataStock.run, loop)
	future_Screen = loop.run_in_executor(None, DataScreen.run, loop)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		print('\r')
		logger.info('Detected KeyboardInterrupt')
		Yahoo.running = False
		NASDAQ.running = False
		DataStock.running = False
		DataScreen.running = False
	while not DataScreen.fin:
		pass
	loop.close()
	logger.info('Fin.')


if __name__ == "__main__":
	main()
