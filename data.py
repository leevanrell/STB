#!/usr/bin/python3
"""
	Data collection manager for STB. Runs several async threads to query and collect data from various api
	Each api typically get its own database
	Currently collecting RSS data from yahoo.finance, Stock prices and indicatos from alphavantage, and wikipedia view counts.
"""
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
import logging
import datetime
import asyncio

from lib.GenericRSS_Data import RSS
from lib.Stock_Data import Stock
from lib.Wiki_Data import Wiki
from lib.Screen_Data import Screen

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')


fh1 = logging.FileHandler('./data/logs/%s.data_debug.log' % (datetime.datetime.now().strftime('%Y-%m-%d')))
fh1.setLevel(logging.DEBUG)
fh1.setFormatter(fmt)
logger.addHandler(fh1)


fh2 = logging.FileHandler('./data/logs/data.log')
fh2.setLevel(logging.INFO)
fh2.setFormatter(fmt)
logger.addHandler(fh2)

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(fmt)
# logger.addHandler(ch)

Alpha_api_key = '2RPX5G5M7XOXMDJU'

Ticker_file = './data/ticker.txt'
RSS_DB_file = './data/data.db'
Stock_DB_file = './data/stock.db'
Wiki_DB_file = './data/wiki.db'

Processes_Count = 4
Fin_TIMEOUT = 5
VERSION = '0.2'


def main():
	Ticker_Companies = [line.strip().split(' : ')[0] for line in open(Ticker_file).readlines()]
	Wikipedia_Companies = [line.strip().split(' : ')[1] for line in open(Ticker_file).readlines()]
	logger.debug('Looking up RSS on %s' % str(Ticker_Companies ))

	loop = asyncio.get_event_loop()
	executor = ProcessPoolExecutor()

	Yahoo_Data = RSS(logger, RSS_DB_file, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Ticker_Companies)
	Stock_Data = Stock(logger, Stock_DB_file, Alpha_api_key, Ticker_Companies )
	Wiki_Data = Wiki(logger, Wiki_DB_file, Wikipedia_Companies)
	Screen_Data = Screen(VERSION, Yahoo_Data, Stock_Data, Wiki_Data)

	future_Yahoo = loop.run_in_executor(None, Yahoo_Data.run, loop)
	future_Stock = loop.run_in_executor(None, Stock_Data.run, loop)
	future_Wiki = loop.run_in_executor(None, Wiki_Data.run, loop)
	future_Screen = loop.run_in_executor(None, Screen_Data.run, loop)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		print('\r')
		logger.info('Detected KeyboardInterrupt')
		Yahoo_Data.running = False
		Stock_Data.running = False
		Screen_Data.running = False
	while not Screen_Data.fin:
		pass
	loop.close()
	logger.info('Fin.')


if __name__ == "__main__":
	main()
