#!/usr/bin/python3
"""
	Data collection manager for STB. Runs several async threads to query and collect data from various api
	Each api typically get its own database
	Currently collecting RSS data from yahoo.finance, Stock prices and indicatos from alphavantage, and wikipedia view counts.
"""
__VERSION__ = '0.4'


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
from time import sleep
from urllib.request import urlopen
from concurrent.futures import ProcessPoolExecutor
from elasticsearch import Elasticsearch

#from lib.Parallel import run as Basics
from lib.Basic_Data import Basics
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
sh1.setLevel(logging.DEBUG)
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
	initDB()
	if args.purge:
		deleteDB()
	if args.init:
		initSTB()
	if args.stats:
		stats()

	if args.rss or  args.wiki:
		#watch_list = getWatchList()
		q = asyncio.Queue()

		ES_thread = Upload(logger, server, 5, q)
		Threads = [ES_thread]
		if args.wiki:
			Wiki_thread = Wiki(logger, index_name[3], q, watch_list)
			Threads.append(Wiki_thread)
		if args.rss:
			RSS_thread = RSS(logger, index_name[4], q, watch_list)
			Threads.append(RSS_thread)

		loop = asyncio.get_event_loop()
		for thread in Threads:
			loop.run_in_executor(None, thread.run, loop)

		try:
			loop.run_forever()
		except KeyboardInterrupt:
			pass
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
	q = asyncio.Queue()

	stream = urlopen(NASDAQ_url)
	NASDAQ = list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	del NASDAQ[0]

	stream = urlopen(NYSE_url)
	NYSE= list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	del NYSE[0]

	Companies = list(NASDAQ + NYSE)
	#Companies = list(divide_chunks(Companies, Threads_count))
	
	loop = asyncio.get_event_loop()

	threads = []
	thread = Basics(logger, index, q, 1, Companies)
	threads.append(thread)


	ES_thread = Upload(logger, q, server, bulk_size, threads)

	asyncio.run(thread.run(), debug=True)
	asyncio.run(ES_thread.run(), debug=True)



	logger.info('Gathering Basic info on NASDAQ and NYSE companies')

	# broker = Broker(max_tries=1, loop=loop)
	# broker.serve(host=proxy_host, port=proxy_port, types=proxy_types, limit=10, max_tries=3,
	# 	prefer_connect=True, min_req_proxy=5, max_error_rate=0.5,
	# 	max_resp_time=8, http_allowed_codes=proxy_codes, backlog=100)


	while not ES_thread.Fin:
		sleep(10)
	#broker.stop()
	loop.close()

def stats():
	tickers = getField('symbol')

	q = asyncio.Queue()

	loop = asyncio.new_event_loop()
	threads = []
	futures = []

	thread = Stats(logger, index, q, tickers)
	threads.append(thread)
	futures.append(loop.run_in_executor(None, thread.run, loop))

	ES_thread = Upload(logger, q, server, bulk_size, threads)
	futures.append(loop.run_in_executor(None, ES_thread.run, loop))

	logger.info('Gathering Indicators on NASDAQ and NYSE companies')

	for future in futures:
		loop.run_until_complete(future)
	while not ES_thread.Fin:
		sleep(10)
	loop.close()


def initDB():
	logger.info('Initializing index %s at %s' % (index, server))
	es = Elasticsearch([server])
	es.indices.create(index=index, ignore=400, body=es_mapping)


def deleteDB():
	logger.info('Purging index %s at %s' % (index, server))
	es = Elasticsearch([server])
	es.indices.delete(index=index, ignore=404)


def divide_chunks(seq, num): 
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def getField(field):
	es = Elasticsearch([server])
	res = es.search(index=index, body={"query": {"match_all": {}}})
	fields = []
	for hit in res['hits']['hits']:
		fields.append(hit['_source'][field])



if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Data Collector for STB')
	parser.add_argument('--purge', dest='purge', action='store_true')
	parser.set_defaults(purge=False)
	parser.add_argument('--init', dest='init', action='store_true')
	parser.set_defaults(init=False)
	parser.add_argument('--rss', dest='rss', action='store_true')
	parser.set_defaults(rssd=False)
	parser.add_argument('--stats', dest='stats', action='store_true')
	parser.set_defaults(statsd=False)
	parser.add_argument('--wiki', dest='wiki', action='store_true')
	parser.set_defaults(wikid=False)
	main(parser.parse_args())
