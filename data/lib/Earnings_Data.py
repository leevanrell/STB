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
import datetime

from time import sleep, time
from bs4 import BeautifulSoup
from datetime import timedelta
from random import choice
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from pyti.exponential_moving_average import exponential_moving_average as ema
from pyti.smoothed_moving_average import smoothed_moving_average as sma
from pyti.relative_strength_index import relative_strength_index as rsi
from pyti.moving_average_convergence_divergence import moving_average_convergence_divergence as macd

try:
	from .Thread import Thread
except:
	from Thread import Thread


class Earnings(Thread):

	def __init__(self, logger, index, q):
		super().__init__('Basics_%s' % thread_number, logger)
		self.q = q
		self.index = index
		self.STOCKS = stocks
		self.RATELIMIT = 30
		self.Yahoo = 'https://finance.yahoo.com/calendar/earnings?day={0}&offset={1}&size=100'


	async def run(self):
		self.logger.info('[%s] Starting thread' % self.Name)
		three_months = datetime.datetime.now() + datetime.timedelta(days=365.25/2)
		delta = three_months - datetime.now()
		date_range = [d1 + timedelta(i) for i in range(delta.days+1)]
		for day in date_range:
			query = self.Yahoo.format(day.strftime('%Y/%m/%d'), 0)
			print(res)
			res = requests.get(self.Zacks_url + stock, headers=self.random_useragent())


			#getsize
			#getstocks
			#queueDoc
			# for i in range(100, size, 100):
			# 	stockd = {}
			# 	stockd['upload_date'] = datetime.now().strftime('%m/%d/%Y')
			# 	query = self.Yahoo.format(date, i)		
			#	getstocks
			#	queueDoc
			#	self.queueDoc(stockd)
		
		self.logger.info('[%s] Finished thread' % self.Name)
		self.Fin = True


	async def queueDoc(self, stockd):
		doc = {}
		doc['_index'] = self.index
		doc['_type'] = 'data_basic'
		doc['_source'] = stockd
		await self.q.put(doc)
		self.logger.debug('[%s] queued document for %s' % (self.Name, doc['_source']['symbol']))


	def random_useragent(self):
		user_agent_list = [
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
			'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
			'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
			'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
			'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
			'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
			'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
			'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
		]
		return {'User-Agent': choice(user_agent_list),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}
 
 
if __name__ == "__main__":


	#https://finance.yahoo.com/calendar/earnings?day=2019-04-30&offset=0&size=277


	# API_key = '4MZB0QBMD26V1F76'

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	# NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
	db_name = 'stock'

	# stream = urllib.request.urlopen(NASDAQ_url)
	# csvfile = list(csv.reader(codecs.iterdecode(stream, 'utf-8')))
	# del csvfile[0]


	# import asyncio
	q = asyncio.Queue()
	s = Earnings(logger, db_name, q)
	# #s = Stats(logger, db_name, q, ['AAPL'],  API_key)
	# s.run('3')
	# ZACKS $("a.dt-button.buttons-csv.buttons-html5").click()