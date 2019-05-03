#!/usr/bin/python3
"""
	RSS data colletion thread
	Gets RSS articles relating to provided tickers and inserts into sqlite3 database
"""

from time import gmtime, strftime, sleep
from datetime import datetime
from bs4 import BeautifulSoup
import feedparser
import sys
import sqlite3
import logging
import requests
import numpy as np
#import .Thread

try:
	from .Thread import Thread
except:
	from Thread import Thread


class RSS(Thread):

	def __init__(self, logger, index, q, tickers):
		super().__init__('RSS', logger)
		#self.server = 
		self.finviz_url = 'https://finviz.com/quote.ashx?t='
		self.Tickers = tickers
		self.delays = [4, 2, 1, 5]
		#self.initDB()


	async def run(self):
		self.logger.info('[RSS] Starting RSS thread')
		for ticker in self.Tickers:
			headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',}
			res = requests.get(self.finviz_url + ticker, headers=headers)
			soup = BeautifulSoup(res.content, "lxml")

			t = soup.find("table", {'id':'news-table'})
			for row in t.findAll("tr"):
				cells = row.findAll("td")
				timestamp = self.getTimestamp(cells[0].find(text=True))
				article_title = cells[1].find(text=True)
				rssd = {'symbol': ticker, 'time' : timestamp, 'article_title' : article_title} 
				await self.queueDoc(rssd)
				
			sleep(np.random.choice(self.delays) * 0.1)
		self.logger.info('[%s] Exiting' % self.Name)
		self.Fin = True


	async def queueDoc(self, rssd):
		doc = {}
		doc['_index'] = self.index
		doc['_type'] = 'data_rss'
		doc['_source'] = rssd
		await self.q.put(doc)
		self.logger.debug('[%s] queued document for %s' % (self.Name, doc['_source']['symbol']))


	def getTimestamp(self, time_raw):
		time_raw = time_raw.replace(u'\xa0', u'').strip()
		if len(time_raw) > 9:
			time_raw = time_raw.split(' ')
			date = time_raw[0]
			time_of_day = time_raw[1]
		else:
			time_of_day = time_raw
		return datetime.strptime(date + ' ' + time_of_day, '%b-%d-%y %I:%M%p').strftime('%m/%d/%Y %H:%M:%S')


if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	import asyncio
	q = asyncio.Queue()

	s = RSS(logger, 'stock', q, ['MSFT', 'APPL'])
	s.run('3')
