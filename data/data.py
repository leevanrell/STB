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
import sys
import codecs
import numpy
import csv
from urllib.request import urlopen
from concurrent.futures import ProcessPoolExecutor
from elasticsearch import Elasticsearch

from lib.Proxy_Basic_Data import Basics
from lib.Stats_Data import Stats
from lib.RSS_Data import RSS
from lib.Upload_Data import Upload
from lib.Wiki_Data import Wiki
from conf.config import *

PATH = os.path.dirname(os.path.abspath(__file__))


#logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(threadName)-11s - %(levelname)s - %(message)s')

sh1 = logging.StreamHandler()
sh1.setLevel(logging.INFO)
sh1.setFormatter(fmt)
logger.addHandler(sh1)

fh2 = logging.FileHandler('%s/conf/logs/data.log' % (PATH))
fh2.setLevel(logging.DEBUG)
fh2.setFormatter(fmt)
logger.addHandler(fh2)

fh2 = logging.FileHandler('%s/conf/logs/error.log' % (PATH))
fh2.setLevel(logging.ERROR)
fh2.setFormatter(fmt)
logger.addHandler(fh2)


def main(args):
	logger.info('Starting STB v%s ' % __VERSION__)

	if args.init:
		logger.info('Performing Setup')
		initSTB()
	if args.rssd or args.statsd or args.wikid:
		watch_list = getWatchList()
		q = asyncio.Queue()

		ES_thread = Upload(logger, server, 5, q)
		Threads = [ES_thread]
		if args.statsd:
			Stats_thread = Stats(logger, index_name[2], q, watch_list, Alpha_API_Key)
			Threads.append(Stats_thread) 
		if args.wikid:
			Wiki_thread = Wiki(logger, index_name[3], q, watch_list)
			Threads.append(Wiki_thread)
		if args.rssd:
			RSS_thread = RSS(logger, index_name[4], q, watch_list)
			Threads.append(RSS_thread)

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

	stream = urlopen(NASDAQ_url)
	NASDAQ = list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	del NASDAQ[0]

	stream = urlopen(NYSE_url)
	NYSE= list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	del NYSE[0]

	Companies = list(NASDAQ + NYSE)
	Companies = list(divide_chunks(Companies, Threads_count))

	ES_thread = Upload(logger, server, 5, q)
	Threads = [ES_thread]
	for i in range(0, Threads_count):
		thread = Basics(logger, index_names[0], q, i, Companies[i])
		Threads.append(thread)
	sys.exit(0)
	loop = asyncio.get_event_loop()
	# #executor = ProcessPoolExecutor(Threads_count)
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

	# es.indices.delete(index=index_names[0], ignore=404)
	# es.indices.delete(index=index_names[1], ignore=404)
	# es.indices.delete(index=index_names[2], ignore=404)
	# es.indices.delete(index=index_names[3], ignore=404)
	# es.indices.delete(index=index_names[4], ignore=404)
	es.indices.create(index=index_names[0], ignore=400, body=es_mappings[0])
	es.indices.create(index=index_names[1], ignore=400, body=es_mappings[1])
	es.indices.create(index=index_names[2], ignore=400, body=es_mappings[2])
	es.indices.create(index=index_names[3], ignore=400, body=es_mappings[3])
	es.indices.create(index=index_names[4], ignore=400, body=es_mappings[4])


def getWatchlist():
	pass

# FIX
def waitforThreads(threads):
	for thread in threads[1:]:
		while not thread.Fin:
			sleep(5)
	threads[0].Running = False
	while not threads[0].Fin:
		sleep(1)
	logger.info('Fin.')


def divide_chunks(l, n): 
      
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 


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
