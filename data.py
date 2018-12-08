#!/usr/bin/python3
"""
	Data collection manager for STB. Runs several async threads to query and collect data from various api
	Each api typically get its own database
	Currently collecting RSS data from yahoo.finance, Stock prices and indicatos from alphavantage, and wikipedia view counts.
"""

#from concurrent.futures import ProcessPoolExecutor
#import concurrent.futures
import logging
import datetime
import asyncio
import argparse

from src.data.GenericRSS_Data import RSS
#from src.data.Stock_Data import Stock
from src.data.Wiki_Data import Wiki
from src.data.Screen_Data import Screen

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')

fh1 = logging.FileHandler('./src/logs/%s.log' % (datetime.datetime.now().strftime('%Y-%m-%d')))
fh1.setLevel(logging.DEBUG)
fh1.setFormatter(fmt)
logger.addHandler(fh1)

fh2 = logging.FileHandler('./src/logs/data.log')
fh2.setLevel(logging.INFO)
fh2.setFormatter(fmt)
logger.addHandler(fh2)

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(fmt)
# logger.addHandler(ch)

#Alpha_api_key = '2RPX5G5M7XOXMDJU'

Ticker_file = './src/conf/ticker.txt'
RSS_DB_file = './src/conf/rss.db'
Stock_DB_file = './src/conf/stock.db'
Wiki_DB_file = './src/conf/wiki.db'

Processes_Count = 4
Fin_TIMEOUT = 5
VERSION = '0.2'


def main(verbose):
	logger.info('Starting STB v%s Data Collector' % VERSION)
	loop = asyncio.get_event_loop()
	#executor = ProcessPoolExecutor()
	Yahoo_Data = RSS(logger, RSS_DB_file, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Ticker_file)
	Wiki_Data = Wiki(logger, Wiki_DB_file, Ticker_file)
	#Stock_Data = Stock(logger, Stock_DB_file, Alpha_api_key, Ticker_file)

	future_Yahoo = loop.run_in_executor(None, Yahoo_Data.run, loop)
	future_Wiki = loop.run_in_executor(None, Wiki_Data.run, loop)
	#future_Stock = loop.run_in_executor(None, Stock_Data.run, loop)
	#Threads = [Yahoo_Data, Stock_Data, Wiki_Data]
	Threads = [Yahoo_Data, Wiki_Data]

	Screen_Data = None
	if verbose:
		Screen_Data = Screen(logger, VERSION, Threads)
		future_Screen = loop.run_in_executor(None, Screen_Data.run, loop)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		stopThreads(Threads, Screen_Data)
	waitforThreads(Threads, Screen_Data)
	loop.close()


def stopThreads(threads, screen):
	print('\r')
	logger.info('Detected KeyboardInterrupt')
	for thread in threads:
		thread.Running = False
	if screen:
		screen.Running = False


def waitforThreads(threads, screen):
	for thread in threads:
		while not thread.Fin:
			pass
	if screen:
		while not screen.Fin:
			pass
	logger.info('Fin.')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Data Collector for STB')
	parser.add_argument('--verbose', dest='verbose', action='store_true')
	parser.set_defaults(verbose=False)
	main(parser.parse_args().verbose)
