#!/usr/bin/python3
"""
	RSS data colletion thread
	Gets RSS articles relating to provided tickers and inserts into sqlite3 database
	@params
		logger: logging object
		DB_file: file location for sqlite3 database
		Website: name of website for thread, used as a table field, incase you want to collect from multiple RSS feeds
		Link: RSS link for collecting api
		Tickers: list of company tickers for data collection
"""

from time import gmtime, strftime, sleep
from datetime import datetime
from bs4 import BeautifulSoup
import feedparser
import sys
import sqlite3
import logging

from data.Thread import Thread

sys.path.append('../')


class RSS(Thread):

	def __init__(self, logger, DB_file, Website, Link, Ticker_file):
		super().__init__('RSS', logger)
		self.DB_file = DB_file
		self.Website = Website
		self.Link = Link
		self.Ticker_file = Ticker_file
		self.Tickers = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
		self.TIMEOUT = 60 * 60
		self.initDB()

	def run(self, loop):
		self.logger.info('[RSS] Starting Stock thread')
		while self.Running:
			try:
				for ticker in self.Tickers:
					feed = feedparser.parse(self.Link + ticker)
					documents = self.getArticles(feed.entries, ticker)
					self.insertDB(documents)
					self.logger.info('[%s] Collected data on %s' % (self.Name,ticker))
				self.logger.debug('[%s] sleeping for %s' % (self.Name, self.TIMEOUT))
				self.wait()
			except Exception as e:
				self.logger.error('[%s] error: %s' % (self.Name, e))
		self.logger.info('[%s] Exiting' % self.Name)
		self.Fin = True

	def getArticles(self, entries, ticker):
		articles = []
		for entry in entries:
			time = self.getTime(entry.published)
			title = self.getTitle(entry)
			summary = self.getSummary(entry)
			if len(summary) > 25:
				article = [time, self.Website, ticker, title, summary]
				articles.append(article)
			else:
				self.logger.debug('[%s] Summary too short: %s' % (self.Name, summary))
		return articles

	def getTitle(self, data):
		return data.title.replace('&apos;', '\'').replace('&quote;', '\'')

	def getSummary(self, data):
		try:
			return BeautifulSoup(data['summary'], 'lxml').text
		except Exception as e:
			self.logger.error('[%s] Bad Entry: %s' % (self.Name, data['summary'][0:15]))
			return ':('

	def getTime(self, data):
		try:
			time = datetime.strptime(data, '%a, %d %b %Y %H:%M:%S +%f').strftime('%Y-%m-%d')
			return time
		except Exception as e:
			pass
		try:
			time = datetime.strptime(data, '%a, %d %b %Y %H:%M:%S Z').strftime('%Y-%m-%d')
			return time
		except Exception as e:
			raise e

	def insertDB(self, documents):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.executemany("""INSERT OR IGNORE INTO RSS (t, website, ticker, title, summary) VALUES (?, ?, ?, ?, ?)""", documents)
		conn.commit()
		c.close()
		conn.close()

	def initDB(self):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.execute("""CREATE TABLE IF NOT EXISTS RSS (t text, website text, ticker text, title text  PRIMARY KEY, summary text)""")	
		conn.commit()
		c.close()
		conn.close()

	def isTickerChanged(self):
		Tickers = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
		if Tickers != self.Tickers:
			logger.debug('[%s] Detected Ticker.txt Change, rerunning' % self.Name)
			self.Tickers = Tickers
			return True
		return False

	def wait(self):
		for s in range(0, self.TIMEOUT):
			if not self.Running:
				break
			if self.isTickerChanged():
				break
			sleep(1)

if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	s = RSS(logger, 'test_rss.db', 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', ['MSFT', 'APPL'])
	s.run('3')
