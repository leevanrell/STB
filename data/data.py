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


import lib
#from lib.RSS_Data import RSS
#from lib.Screen_Data import Screen
from lib.Stocks_Data import Basics
#from lib.Stocks_Data import Stats
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


def main(verbose):
	logger.info('Starting STB v%s ' % __VERSION__)

	initDB()

	loop = asyncio.get_event_loop()
	q = asyncio.Queue()


	NASDAQ_thread = Basics(logger, NASDAQ_url, Quandl_api_key db_name, q)
	#NYSE_thread = Basics(logger, NYSE_url, index_name, 'data_basic', q)
	#ES_thread = Upload(logger, server, index_name, q)
	
	loop.run_in_executor(None, NASDAQ_thread.run, loop)
	loop.run_in_executor(None, ES_thread.run, loop)


	#loop.run_in_executor(None, NYSE_thread.run, loop)

	#executor = ProcessPoolExecutor()
	#loop.run_in_executor(executor, NYSE_thread.run, loop)

	loop.run_forever()

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

def initDB():
	mappings = {
		"settings": {
			"number_of_shards" :   5,
			"number_of_replicas" : 1
		},
		"mappings": {
			"data_basic": {
				"properties": {
					"symbol": {"type": "keyword"},
					"name": {"type": "text"},		
					"price_close": {"type": "float"},
					"mkt_cap": { "type": "integer"},
					"avg_volume": {"type": "integer"},
					"sector": {"type": "text"},	
					"industry": {"type": "text"},	
					'upload_date': { 
						"type": "date",
						"format": "MM/dd/yyyy"
					},	
					"IPO_year": { 
						"type": "date",
						"format": "yyyy"
					},	
					"next_earnings" : { 
						"type": "date",
						"format": "MM/dd/yyyy"
					},
				}
			},
			"data_stats": {
				"properties": { 
					"symbol": { "type": "keyword"},
					"price_last": { "type": "float"},
					"date": { 
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
					"price_open": { "type": "float"},
					"price_high": { "type": "float"},
					"price_low": { "type": "float"},
					"price_close": {"type": "float"},
					"vol": { "type": "integer"},                
					"EMA20": {"type": "float"},
					"SMA20": {"type": "float"},
					"SMA50": {"type": "float"},                
					"SMA200": {"type": "float"},
					"RSI": {"type": "float"},
					"MACD": {"type": "float"}
				}
			},
			"data_wiki": {
				"properties": {
					"symbol": {"type": "keyword"},
					"company_name": {"type": "text"},
					"wiki_views": {"type": "integer"},
					"date": {
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
				}
			},
			"data_rss": {
				"properties": {
					"article": {"type" : "text"},
					"article_date": {
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
					"upload_date": {
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
				}
			}
		}
	}

	es = Elasticsearch([server])
	if not es.indices.exists(index="stock"):
		es.indices.create(index=index_name, ignore=400, body=mappings)
	else: 
		######remove in future
		es.indices.delete(index=index_name, ignore=[400, 404])
		es.indices.create(index=index_name, ignore=400, body=mappings)



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