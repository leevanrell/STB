#!/usr/bin/python3
"""
	Data collection manager for STB. Runs several async threads to query and collect data from various api
	Each api typically get its own database
	Currently collecting RSS data from yahoo.finance, Stock prices and indicatos from alphavantage, and wikipedia view counts.
"""
__VERSION__ = '0.3'


#from concurrent.futures import ProcessPoolExecutor
#import concurrent.futures
import logging
import datetime
import asyncio
import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor
from elasticsearch import Elasticsearch

from lib.Stock_Data import Basics, Stats
from lib.RSS_Data import RSS
from lib.Upload_Data import Upload
from lib.Wiki_Data import Wiki

import conf.config

PATH = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(threadName)-11s - %(levelname)s - %(message)s')

sh1 = logging.StreamHandler()
sh1.setLevel(logging.DEBUG)
sh1.setFormatter(fmt)
logger.addHandler(sh1)

# fh2 = logging.FileHandler('%s/conf/logs/data.log' % (PATH))
# fh2.setLevel(logging.INFO)
# fh2.setFormatter(fmt)
# logger.addHandler(fh2)


def main(args):
	logger.info('Starting STB v%s ' % __VERSION__)

	if args.init:
		logger.info('Performing Setup')
		initSTB()
	if args.rssd or args.statsd or args.wikid:
		watch_list = getWatchList()
		q = asyncio.Queue()
		Threads = [Upload(logger, server, index, q)]

		if args.rssd:
			RSS_thread = RSS(logger, index, q, watch_list)
			Threads.append(RSS_thread)
		if args.statsd:
			Stats_thread = Stats(logger, index, q, watch_list, Alpha_API_Key)
			Threads.append(Stats_thread) 
			logger, DB_file, Ticker_file
		if args.wikid:
			Wiki_thread = Wiki(logger, index, q, watch_list)
			Threads.append(Wiki_thread)

		loop = asyncio.get_event_loop()
		for thread in Threads:
			loop.run_in_executor(None, thread.run, loop)

		loop.run_forever()
		waitforThreads(Threads)
		ES_thread.Running = False
		waitforThreads([ES_thread])

		try:
			loop.run_forever()
		except KeyboardInterrupt:
			stopThreads(Threads)
		loop.close()

	#companies_list = split_companies(Proc_Count)
	#Yahoo_Data = RSS(logger, RSS_DB_file, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Ticker_file)
	#Stock_Data = Stock(logger, Stock_DB_file, Alpha_api_key, Ticker_file)
	#Stock_Wiki= Wiki(logger, Wiki_DB_file, Companies)
	# Threads = []
	# for i in range(0, Proc_Count):
	# 	w = Wiki(logger, Wiki_DB_file, companies_list[i])
	# 	future_w = loop.run_in_executor(None, w.init_db, loop)
	# 	Threads.append(w)
	
	# #future_Yahoo = loop.run_in_executor(None, Yahoo_Data.run, loop)
	# #future_Stock = loop.run_in_executor(None, Stock_Data.run, loop)
	# #future_Wiki = loop.run_in_executor(None, Wiki_Data.run, loop)

	# #Threads = [Yahoo_Data, Stock_Data, Wiki_Data]
	# #Threads = [Yahoo_Data, Wiki_Data]

	# Screen_Data = None
	# if verbose:
	# 	Screen_Data = Screen(logger, VERSION, Threads)
	# 	future_Screen = loop.run_in_executor(None, Screen_Data.init_db, loop)
	# try:
	# 	loop.run_forever()
	# except KeyboardInterrupt:
	# 	stopThreads(Threads, Screen_Data)
	# waitforThreads(Threads, Screen_Data)
	# loop.close()


def initSTB():
	initDB()

	q = asyncio.Queue()

	NASDAQ_thread = Basics(logger, index, q, NASDAQ_url)
	NYSE_thread = Basics(logger, index, q, NYSE_url)
	ES_thread = Upload(logger, server, index, q)
	Threads = [ES_thread, NASDAQ_thread, NYSE_thread]

	loop = asyncio.get_event_loop()
	#executor = ProcessPoolExecutor()
	for thread in Threads:
		loop.run_in_executor(None, thread.run, loop)

	logger.info('Gathering Basic info on NASDAQ and NYSE companies')
	loop.run_forever()
	waitforThreads(Threads)
	ES_thread.Running = False
	waitforThreads([ES_thread])
	loop.close()


def initDB():
	logger.info('Initializing Elasticsearch at %s' % server)

	es = Elasticsearch([server])
	if not es.indices.exists(index="stock"):
		es.indices.create(index=index, ignore=400, body=es_mappings)
	# else: 
	# 	es.indices.delete(index=index, ignore=[400, 404])
	# 	es.indices.create(index=index, ignore=400, body=es_mappings)

def getWatchlist():
	pass

def waitforThreads(threads):
	for thread in threads[1:]:
		while not thread.Fin:
			sleep(5)
	threads[0].Running = False
	while not threads[0].Fin:
		sleep(1)
	logger.info('Fin.')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Data Collector for STB')
	parser.add_argument('--init', dest='init', action='store_true')
	parser.set_defaults(init=False)
	parser.add_argument('--rssd', dest='rssd', action='store_true')
	parser.set_defaults(rssd=False)
	parser.add_argument('--statsd', dest='statsd', action='store_true')
	parser.set_defaults(statsd=False)
	parser.add_argument('--wikid', dest='wikid', action='store_true')
	parser.set_defaults(wikid=False)
	main(parser.parse_args())
