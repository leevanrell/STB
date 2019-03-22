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
import requests
import numpy as np
#import .Thread
from Thread import Thread


class RSS(Thread):

	def __init__(self, logger, tickers):
		super().__init__('RSS', logger)
		#self.server = 
		self.url = 'https://finviz.com/quote.ashx?t='
		self.Tickers = tickers
		self.delays = [4, 2, 1, 5]
		#self.initDB()

	def run(self, loop):
		self.logger.info('[RSS] Starting Stock thread')
		while self.Running:
			#try:
			for ticker in self.Tickers:
				ticker_url = self.url + ticker
				headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',}
				res = requests.get(ticker_url, headers=headers)
				soup = BeautifulSoup(res.content, "lxml")

				# t = soup.findAll("table")[7]
				# print(t)
				t = soup.find("table", {'id':'news-table'})
				for row in t.findAll("tr"):
					cells = row.findAll("td")
					print(cells[0].find(text=True))
					print(cells[1].find(text=True))
				#print(t)
				self.Running = False
				break

				#self.insertDB(document)
				self.logger.info('[%s] Collected data on %s' % (self.Name,ticker))
				sleep(np.random.choice(self.delays) * 0.1)
			#except Exception as e:
				#self.logger.error('[%s] error: %s' % (self.Name, e))
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

	def getTickers(self):
		pass

	def insertDB(self, documents):
		pass

	# def isTickerChanged(self):
	# 	Tickers = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
	# 	if Tickers != self.Tickers:
	# 		logger.debug('[%s] Detected Ticker.txt Change, rerunning' % self.Name)
	# 		self.Tickers = Tickers
	# 		return True
	# 	return False

	# def wait(self):
	# 	self.logger.debug('[%s] sleeping for %s' % (self.Name, self.TIMEOUT))
	# 	for s in range(0, self.TIMEOUT):
	# 		if not self.Running:
	# 			break
	# 		if self.isTickerChanged():
	# 			break
	# 		sleep(1)

if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	s = RSS(logger, ['MSFT', 'APPL'])
	s.run('3')
