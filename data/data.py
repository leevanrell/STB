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
import os
import time

import lib
from elasticsearch import Elasticsearch
from lib.Stocks_Data import Basics
from lib.Stocks_Data import Stats
from lib.GenericRSS_Data import RSS
from lib.Screen_Data import Screen
from lib.Wiki_Data import Wiki

import conf
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


Alpha_api_key = '2RPX5G5M7XOXMDJU'
db_name = 'stock'
Proc_Count = 16
VERSION = '0.3'

NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
NYSE_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
		

def main(verbose):
	logger.info('Starting STB v%s ' % VERSION)

	initDB()
	basics = Basics(logger)
	s = time.time()
	companies = basics.run()
	print(companies)
	print(time.time() - s)
	loop = asyncio.get_event_loop()


	NASDAQ_thread = Basics(logger, NASDAQ_url)
	NYSE_thread = Basics(logger, NYSE_url)
	
	loop.run_in_executor(None, NASDAQ_thread.run, loop)
	loop.run_in_executor(None, NYSE_thread.run, loop)

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
					"wiki_views": {"type": "intger"},
					"date": {
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
				}
			},
			"data_basic": {
				"properties": {
					"symbol": {"type": "keyword"},
					"name": {"type": "text"},
					"sector": {"type": "text"},			
					"price_close": {"type": "float"},
					"mkt_cap": { "type": "integer"},
					"avg_volume": {"type": "integer"},
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
			}
		}
	}

	
	es.indices.delete(index='stock', ignore=[400, 404])
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	if not es.indices.exists(index="stock"):
		es.indices.create(index=db_name, ignore=400, body=mappings)
	else: 
		######remove in future
		es.indices.delete(index=db_name, ignore=[400, 404])
		es.indices.create(index=db_name, ignore=400, body=mappings)


# def split_companies(p):
# 	with open(Company_file, 'r') as f:
# 		companies = [ l.strip() for l in f.readlines()]
# 	n = int(len(companies) / p) 
# 	companies_list = [companies[i * n:(i + 1) * n] for i in range((len(companies) + n - 1) // n )] 
# 	return companies_list


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
