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

from data.Thread import Thread

sys.path.append('../')


class Wiki(Thread):

	def __init__(self, logger, DB_file, Ticker_file):
		super().__init__('Wiki', logger)
		self.DB_file = DB_file
		self.Ticker_file = Ticker_file
		Ticker_Companies = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
		Wikipedia_Articles = [line.strip().split(' : ')[1] for line in open(self.Ticker_file).readlines()]
		self.Tickers = dict(zip(Ticker_Companies, Wikipedia_Articles))
		self.TIMEOUT = 60 * 60 * 24
		self.Start_date = '20090101'
		self.initDB()

	def run(self, loop):
		viewer = PageviewsClient(user_agent="<person@organization.org> Selfie, Cat, and Dog analysis")
		self.logger.info('[%s] Starting Stock thread' % self.Name)
		while self.Running:
			try:
				for ticker in self.Tickers.keys():
					if not self.Running:
						break
					article = self.Tickers[ticker]
					documents = self.getViews(viewer, ticker, article)
					self.insertDB(documents)
					self.logger.info('[%s] Collected Info on %s' % (self.Name, ticker))
				self.logger.debug('[%s] sleeping for %s' % (self.Name, self.TIMEOUT))
				self.wait()
			except Exception as e:
				self.logger.error('[%s] error: %s' % (self.Name, e))
		self.logger.info('[%s] Exiting' % self.name)
		self.Fin = True

	def getViews(self, viewer, ticker, article):
		End_date = time.strftime('%Y%m%d')
		data = viewer.article_views('en.wikipedia', article, granularity='daily', start=self.Start_date, end=End_date)
		documents = []
		for row in data:
			if data[row][article]:
				document = [row.strftime('%Y-%m-%d'), ticker, int(data[row][article])]
				documents.append(document)
		return documents

	def insertDB(self, documents):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.executemany("""INSERT OR REPLACE INTO Wiki (day, company, views) VALUES (?, ?, ?)""", documents)
		conn.commit()
		c.close()
		conn.close()

	def initDB(self):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.execute("""CREATE TABLE IF NOT EXISTS Wiki (day text, company text, views int, PRIMARY KEY (day, company));""")	
		conn.commit()
		c.close()
		conn.close()

	def wait(self):
		for s in range(0, self.TIMEOUT):
			if not self.Running:
				break
			if self.isTickerChanged():
				break
			sleep(1)

	def isTickerChanged(self):
		Ticker_Companies = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
		Wikipedia_Articles = [line.strip().split(' : ')[1] for line in open(self.Ticker_file).readlines()]
		Tickers = dict(zip(Ticker_Companies, Wikipedia_Articles))
		if Tickers != self.Tickers:
			logger.debug('[%s] Detected Ticker.txt Change, rerunning' % self.Name)
			self.Tickers = Tickers
			return True
		return False


if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	dic = {'AMD': 'Advanced_Micro_Devices', 'MSFT': 'Microsoft'}

	s = Wiki(logger, 'test_wiki.db', dic)
	s.run('3')
