#!/usr/bin/python3

from datetime import datetime
import pandas as pd
import sqlite3


class DF():

	def __init__(self, Stock_file, Wiki_file):
		self.Stock_file = Stock_file
		self.Wiki_file = Wiki_file
		self.data = self.getFrame()

	def getFrame(self):
		df1 = self.getStock('AMD') # get stock and indicators
		df2 = self.getViews('AMD') # get wikipedia views
		result = df2.join(df1) # combine data set

		result = self.normalize(result, 'views') # normalize data

		result = self.normalize(result, 'close')
		#result = replaceNaN(result, 'close') # replace weekend with friday values

		result = self.normalize(result, 'volume')
		result = self.replaceNaN(result, 'volume') # replace weekend with friday values

		result = self.getOneDay(result, 'close')

		return result

	def getStock(self, Company):
		conn = sqlite3.connect(self.Stock_file, timeout=7)
		c = conn.cursor()
		c.execute("""SELECT * from Stock WHERE Company='%s'""" % Company)
		results = c.fetchall()
		c.close()
		conn.close()

		date = []
		#company = []
		close = []
		volume = []
		EMA20 = []
		SMA20 = []
		SMA50 = []
		SMA200 = []
		RSI = []
		MACD = []
		for row in results:
			#date.append(row[0])
			date.append(datetime.strptime(row[0], '%Y-%m-%d'))
			close.append(row[5])
			volume.append(row[6])
			EMA20.append(row[7])
			SMA20.append(row[8])
			SMA50.append(row[9])
			SMA200.append(row[10])
			RSI.append(row[11])
			MACD.append(row[12])

		data = {'close': close, 'volume': volume, 'EMA20': EMA20, 'SMA20': SMA20, 'SMA50': SMA50, 'SMA200': SMA200, 'RSI': RSI, 'MACD': MACD}
		return pd.DataFrame(data, index=date)

	def getViews(self, Company):
		conn = sqlite3.connect(self.Wiki_file, timeout=7)
		c = conn.cursor()
		c.execute("""SELECT * from Wiki WHERE Company='%s'""" % Company)
		results = c.fetchall()
		c.close()
		conn.close()

		date = []
		views = []
		for row in results:
			date.append(datetime.strptime(row[0], '%Y-%m-%d'))
			views.append(row[2] / 90.0)
		return pd.DataFrame({'views': views}, index=date)

	def normalize(self, df, column):
		mean = df[column].mean()
		std = df[column].std()
		df[column] = (df[column] - mean) / std
		return df

	def replaceNaN(self, df, column):
		prev = df.head(1)[column]
		for i, row in df.iterrows():
			if(pd.isnull(row[column])):
				df.at[i, column] = prev
			prev = row[column]
		return df

	def getOneDay(self, df, column):
		prev = df[column].iloc[0]
		date = []
		buy = []
		for i, row in df.iterrows():
			if prev < row[column]:
				buy.append(2) # BUY
			elif prev == row[column]:
				buy.append(1) # HOLD
			else:
				buy.append(0) # SELL
			prev = row[column]
			date.append(i)
		one = pd.DataFrame({'one': buy}, index=date)
		df = df.join(one)
		return df
