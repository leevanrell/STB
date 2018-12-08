#!/usr/bin/python3
"""
	Stock data colletion thread
	Gets daily data on individual companies and inserts into sqlite3 database
	@params
		logger: logging object
		DB_file: file location for sqlite3 database
		API_key: api key for alphavantage
		Tickers: list of company tickers for data collection
"""

from time import sleep
import sys
import urllib.request
import logging
import sqlite3
import json

from data.Thread import Thread

sys.path.append('../')


class Stock(Thread):

	def __init__(self, logger, DB_file, API_key, Ticker_file):
		super().__init__('Stock', logger)
		self.DB_file = DB_file
		self.API_key = API_key
		self.Ticker_file = Ticker_file
		self.Tickers = [line.strip().split(' : ')[0] for line in open(self.Ticker_file).readlines()]
		self.QUERYTIMEOUT = 12
		self.QUERYCOUNT = 7
		self.TIMEOUT = 60 * 60 * 24
		self.CollectionTime = self.QUERYTIMEOUT * self.QUERYCOUNT * len(self.Tickers)
		self.URL = 'http://www.alphavantage.co/query'
		self.initDB()

	def run(self, loop):
		self.logger.info('[%s] Starting Stock thread' % self.Name)
		while self.Running:
			self.logger.info('[%s] Collecting data, expected total runtime: %s (s), %s (s) each' % (
				self.Name, self.CollectionTime, int(self.CollectionTime / len(self.Tickers))))
			try:
				for Ticker in self.Tickers:
					if not self.Running:
						break
					documents = self.getDocuments(Ticker)
					self.insertDB(documents)
					self.logger.info('[%s] Collected Info on %s' % (self.Name, Ticker))
				self.logger.debug('[%s] sleeping for %s' % (self.Name, self.TIMEOUT))
				self.wait()
			except Exception as e:
				self.logger.error('[%s] error: %s' % (self.Name, e))
		self.logger.info('[%s] Exiting' % self.Name)
		self.Fin = True

	def getDocuments(self, ticker):
		Time_series, EMA20_series, SMA20_series, SMA50_series, SMA200_series, RSI_series, MACD_series = self.getSeries(ticker)
		documents = []
		for date in Time_series:
			if int(date.split('-')[0]) > 2008:
				time_open = float(Time_series[date]['1. open'])
				time_high = float(Time_series[date]['2. high'])
				time_low = float(Time_series[date]['3. low'])
				time_close = float(Time_series[date]['4. close'])
				time_volume = int(Time_series[date]['5. volume'])
				EMA20 = float(EMA20_series[date]['EMA'])
				SMA20 = float(SMA20_series[date]['SMA'])
				SMA50 = float(SMA50_series[date]['SMA'])
				SMA200 = float(SMA200_series[date]['SMA'])
				RSI = float(RSI_series[date]['RSI'])
				MACD = float(MACD_series[date]['MACD'])
				document = [date, ticker, time_open, time_high, time_low, time_close, time_volume, EMA20, SMA20, SMA50, SMA200, RSI, MACD]
				documents.append(document)
		return documents

	def getSeries(self, ticker):
		t_dict = {'function': 'TIME_SERIES_DAILY', 'symbol': ticker, 'outputsize': 'full', 'apikey': self.API_key}
		Time_series = self.sendRequest(self.getQuery(t_dict), 'Time Series (Daily)')
		ema20_dict = {'function': 'EMA', 'symbol': ticker, 'interval': 'daily', 'time_period': '20', 'series_type': 'close', 'apikey': self.API_key}
		EMA20_series = self.sendRequest(self.getQuery(ema20_dict), 'Technical Analysis: EMA')
		sma20_dict = {'function': 'SMA', 'symbol': ticker, 'interval': 'daily', 'time_period': '20', 'series_type': 'close', 'apikey': self.API_key}
		SMA20_series = self.sendRequest(self.getQuery(sma20_dict), 'Technical Analysis: SMA')
		sma50_dict = {'function': 'SMA', 'symbol': ticker, 'interval': 'daily', 'time_period': '50', 'series_type': 'close', 'apikey': self.API_key}
		SMA50_series = self.sendRequest(self.getQuery(sma50_dict), 'Technical Analysis: SMA')
		sma200_dict = {'function': 'SMA', 'symbol': ticker, 'interval': 'daily', 'time_period': '200', 'series_type': 'close', 'apikey': self.API_key}
		SMA200_series = self.sendRequest(self.getQuery(sma200_dict), 'Technical Analysis: SMA')
		rsi_dict = {'function': 'RSI', 'symbol': ticker, 'interval': 'daily', 'time_period': '14', 'series_type': 'close', 'apikey': self.API_key}
		RSI_series = self.sendRequest(self.getQuery(rsi_dict), 'Technical Analysis: RSI')
		macd_dict = {'function': 'MACD', 'symbol': ticker, 'interval': 'daily', 'series_type': 'close', 'apikey': self.API_key}
		MACD_series = self.sendRequest(self.getQuery(macd_dict), 'Technical Analysis: MACD')
		return Time_series, EMA20_series, SMA20_series, SMA50_series, SMA200_series, RSI_series, MACD_series

	def getQuery(self, dict):
		url = self.URL + '?'
		for key in dict.keys():
			url += key + '=' + dict[key] + '&'
		return url

	def sendRequest(self, URL, key):
		r = urllib.request.urlopen(URL)
		document = json.loads(r.read().decode('utf-8'))
		document = document[key]
		sleep(self.QUERYTIMEOUT)
		return document

	def getMACD(self, Ticker, interval, series_type):
		url = self.URL +'?function=%s&symbol=%s&interval=%s&series_type=%s&apikey=%s' % (
			'MACD', Ticker, interval, series_type, self.API_key) 
		data = self.getURL(url)
		return data['Technical Analysis: MACD']

	def insertDB(self, documents):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.executemany("""INSERT OR IGNORE INTO Stock (day, company, open, high, low, close, volume, EMA20, SMA20, SMA50, SMA200, RSI, MACD) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", documents)
		conn.commit()
		c.close()
		conn.close()

	def initDB(self):
		conn = sqlite3.connect(self.DB_file)
		c = conn.cursor()
		c.execute("""CREATE TABLE IF NOT EXISTS Stock (day text, company text, open real, high real, low real, close real, volume real, EMA20 real, SMA20 real, SMA50 real, SMA200 real, RSI real, MACD real, PRIMARY KEY (day, company));""")	
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
		if Ticker_Companies != self.Ticker_Companies:
			logger.debug('[%s] Detected Ticker.txt Change, rerunning' % self.Name)
			self.Ticker_Companies = Ticker_Companies
			return True
		return False


if __name__ == "__main__":
	API_key = '2RPX5G5M7XOXMDJU'

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	s = Stock(logger, 'test_stock.db', 'http://www.alphavantage.co/query', API_key, ['MSFT', 'AAPL'])
	s.run('3')
