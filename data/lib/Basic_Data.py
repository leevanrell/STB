#!/usr/bin/python3
"""

"""

import requests
import random
import sys
import logging
import csv
import urllib
import codecs
import numpy
import json
import pandas as pd
from time import sleep, time
from bs4 import BeautifulSoup
from datetime import datetime
#from urllib.request import Request, urlopen
from random import choice

from pyti.exponential_moving_average import exponential_moving_average as ema
from pyti.smoothed_moving_average import smoothed_moving_average as sma
from pyti.relative_strength_index import relative_strength_index as rsi
from pyti.moving_average_convergence_divergence import moving_average_convergence_divergence as macd

try:
	from .Thread import Thread
except:
	from Thread import Thread


class Basics(Thread):

	def __init__(self, logger, index, q, thread_number, stocks):
		super().__init__('Basics_%s' % thread_number, logger)
		self.q = q
		self.index = index
		self.STOCKS = stocks
		self.RATELIMIT = 30


	async def run(self):
		self.logger.info('[%s] Starting thread' % self.Name)
		for line in self.STOCKS:
			try:
				stockd = {}
				stockd['upload_date'] = datetime.now().strftime('%m/%d/%Y')
				stockd['symbol'] = line[0]
				stockd['name'] = line[1]

				try:
					stockd['price_close'] = float(line[2])
				except:
					#stockd['price_close'] = -1.0
					pass

				try:
					stockd['IPO_year'] = int(line[4])
				except:
					#stockd['IPO_year'] = 1776
					pass

				stockd['sector'] = line[5]
				stockd['industry'] = line[6]

				if 'M' in line[3]:
					stockd['mkt_cap'] = float(line[3][1:-1]) * 1e6
				elif 'B' in line[3]:
					stockd['mkt_cap'] =  float(line[3][1:-1]) * 1e9
				else:
					#mkt_cap = None
					pass

				await self.queueDoc(stockd)
			except KeyboardInterrupt:
				raise
			# except Exception as e:
			# 	self.logger.warning('[%s] Error %s encountered at %s' % (self.Name, e, line[0]))

		self.logger.info('[%s] Finished thread' % self.Name)
		self.Fin = True


	async def queueDoc(self, stockd):
		doc = {}
		doc['_index'] = self.index
		doc['_type'] = 'data_basic'
		doc['_source'] = stockd
		self.q.put(doc)
		self.logger.debug('[%s] queued document for %s' % (self.Name, doc['_source']['symbol']))


if __name__ == "__main__":
	API_key = '4MZB0QBMD26V1F76'

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
	db_name = 'stock'

	stream = urllib.request.urlopen(NASDAQ_url)
	csvfile = list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	del csvfile[0]


	import asyncio
	q = asyncio.Queue()
	s = Basics(logger, db_name, q, 1, csvfile)
	#s = Stats(logger, db_name, q, ['AAPL'],  API_key)
	s.run('3')
