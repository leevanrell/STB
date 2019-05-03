#!/usr/bin/python3
"""
	Stock data colletion thread
	Gets daily data on individual companies and inserts into sqlite3 database
"""

import sys
import urllib.request
import logging
import requests
import numpy
import json
import random
import pandas as pd
from time import sleep, time
from bs4 import BeautifulSoup
from datetime import datetime

from pyti.exponential_moving_average import exponential_moving_average as ema
from pyti.smoothed_moving_average import smoothed_moving_average as sma
from pyti.relative_strength_index import relative_strength_index as rsi
from pyti.moving_average_convergence_divergence import moving_average_convergence_divergence as macd

try:
	from .Thread import Thread
except:
	from Thread import Thread


class Stats(Thread):

	def __init__(self, logger, index, q, tickers):
		super().__init__('Stats', logger)
		self.index = index
		self.q = q
		self.tickers = tickers
		self.URL = 'https://api.iextrading.com/1.0/stock/{0}/chart/5y'


	async def run(self):
		self.logger.info('[%s] Starting thread' % self.Name)
		for ticker in self.tickers:
			start = time()
			headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',}
			res = requests.get(self.URL.format(ticker), headers=headers)
			datas = json.loads(res.content)
			await self.parseResponse(ticker, datas)
			end = time()
			if 0.1 > (end - start):
				await sleep(0.1 - (end - start))
		self.logger.info('[%s] Finished thread' % self.Name)
		self.Fin = True


	async def parseResponse(self, ticker, datas):
			timestamp_, open_, high_, low_, close_, volume_ = [], [], [], [], [], []

			for v in datas:
				timestamp_.append(datetime.strptime(v['date'], '%Y-%m-%d').strftime('%m/%d/%Y'))
				open_.append(float(v['open']))
				high_.append(float(v['high']))
				low_.append(float(v['low']))
				close_.append(float(v['close']))
				volume_.append(int(v['volume']))

			try:
				ema20_ = ema(close_, 20)
			except:
				ema20_ = []
				for i in range(0, len(timestamp)):
					ema20_.append(0)

			try:
				sma20_ = sma(close_, 20)
			except:
				sma20_ = []
				for i in range(0, len(timestamp)):
					sma20_.append(0)

			try:
				sma50_ = sma(close_, 50)
			except:
				sma50_ = []
				for i in range(0, len(timestamp)):
					sma50_.append(0)

			try:
				sma200_ = sma(close_, 200)
			except:
				sma200_ = []
				for i in range(0, len(timestamp)):
					sma200_.append(0)

			try:
				rsi_ = rsi(close_, 14)
			except:
				rsi_ = []
				for i in range(0, len(timestamp)):
					rsi_.append(0)			

			try:
				macd_ = macd(close_, 12, 26)
			except:
				macd_ = []
				for i in range(0, len(timestamp)):
					macd_.append(0)				

			for i in range(0, len(timestamp_)):
				statsd = {}
				statsd['symbol'] = ticker
				statsd['date'] = timestamp_[i]
				statsd['price_open'] = open_[i]
				statsd['price_high'] = high_[i]
				statsd['price_low'] = low_[i]
				statsd['price_close'] = close_[i]
				statsd['volume'] = volume_[i]
				statsd['EMA20'] = ema20_[i]
				statsd['SMA20'] = sma20_[i]
				statsd['SMA50'] = sma50_[i]
				statsd['SMA200'] = sma200_[i]
				statsd['RSI'] = rsi_[i]
				statsd['MACD'] = macd_[i]
				await self.queueDoc(statsd)


	async def queueDoc(self, stockd):
		doc = {}
		doc['_index'] = self.index
		doc['_type'] = 'data_stats'
		doc['_source'] = stockd
		await self.q.put(doc)
		self.logger.debug('[%s] queued document for %s' % (self.Name, doc['_source']['symbol']))


if __name__ == "__main__":

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
	db_name = 'stock'

	import asyncio
	q = asyncio.Queue()
	#s = Basics(logger, db_name, q, NASDAQ_url)
	s = Stats(logger, db_name, q, ['AAPL'])
	s.run('3')
