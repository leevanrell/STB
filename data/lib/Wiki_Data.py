#!/usr/bin/python3
"""
	Wikipedia data colletion thread
	Gets daily view counts of provided wikipedia sites and inserts into sqlite3 database
	@params
		logger: logging object
		DB_file: file location for sqlite3 database
		Tickers: list of company wikipedia articles
"""

from time import sleep
from mwviews.api import PageviewsClient
import sys
import logging
import sqlite3
import time

try:
	from .Thread import Thread
except:
	from Thread import Thread


class Wiki(Thread):

	def __init__(self, logger, index, q, tickers):
		super().__init__('Wiki', logger)
		self.q = q
		self.index = index
		self.Tickers = tickers
		self.Start_date = '20090101'


	def run(self):
		viewer = PageviewsClient(user_agent="<person@organization.org> Selfie, Cat, and Dog analysis")
		self.logger.info('[%s] Starting Wiki thread' % self.Name)
		try:
			for ticker, article in self.Tickers.items():
				End_date = time.strftime('%Y%m%d')
				data = viewer.article_views('en.wikipedia', article, granularity='daily', start=self.Start_date, end=End_date)
				for row in data:
					if data[row][article]:
						wikid = {}
						wikid['date'] = row.strftime('%m/%d/%Y')
						wikid['symbol'] = ticker
						wikid['article'] = article
						wikid['wiki_views'] = int(data[row][article])
						queueDoc(wikid)
				self.logger.info('[%s] Collected Info on %s' % (self.Name, ticker))
		except Exception as e:
			self.logger.error('[%s] Error: %s' % (self.Name, e))
		self.logger.info('[%s] Exiting' % self.name)
		self.Fin = True


	def queueDoc(self, wikid):
		doc = {}
		doc['_index'] = self.index
		doc['_type'] = 'data_wiki'
		doc['_source'] = wikid
		self.q.put_nowait(doc)
		self.logger.debug('[%s] queued document for %s' % (self.Name, doc['_source']['symbol']))
		sleep(1/self.RATELIMIT)


if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	dic = {'AMD': 'Advanced_Micro_Devices', 'MSFT': 'Microsoft'}
	import asyncio 
	q = asyncio.Queue()

	s = Wiki(logger, 'stock', q, dic)
	s.run('3')
