#!/usr/bin/python3
from time import gmtime, strftime, sleep
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
import time
import datetime
import os
import logging
import asyncio
import sqlite3

from lib.Generic_RSS import Generic_RSS
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('./data/RSS.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
ch.setFormatter(fmt)
fh.setFormatter(fmt)
logger.addHandler(ch)
logger.addHandler(fh)


Ticker_file = './data/ticker.txt'
DB_file = './data/rss.db'

RSS_TIMEOUT = 60 * 60
SCREEN_REFRESH = 5
Processes_Count = 4
Fin_TIMEOUT = 5
VERSION = '0.1'


def main():
	Companies = [line.strip() for line in open(Ticker_file).readlines()]
	logger.debug('Looking up RSS on %s' % str(Companies))

	global Yahoo, NASDAQ, Screen_Running

	executor = ProcessPoolExecutor()
	loop = asyncio.get_event_loop()

	Yahoo = Generic_RSS(logger, RSS_TIMEOUT, DB_file, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Companies)
	NASDAQ = Generic_RSS(logger, RSS_TIMEOUT, DB_file, 'NASDAQ', 'http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=', Companies)
	Screen_Running = True

	Y = loop.run_in_executor(None, Yahoo.run, loop)
	N = loop.run_in_executor(None, NASDAQ.run, loop)
	S = loop.run_in_executor(None, screen, loop)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		print('\r')
		logger.info('Detected KeyboardInterrupt')
		Yahoo.running = False
		NASDAQ.running = False
		Screen_Running = False
	sleep(Fin_TIMEOUT)
	loop.close()
	logger.info('Fin.')


def screen(loop):
	start_time = datetime.datetime.now()
	while Screen_Running:
		print("Started STB v%s at %s " % (VERSION, time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.localtime())))
		print("Duration: {}".format(datetime.datetime.now() - start_time))
		print("Current Entry Count: %s" % getRowCount())
		sleep(SCREEN_REFRESH)
		os.system( 'clear' )
	print('Fin.')


def getRowCount():
	conn = sqlite3.connect(DB_file)
	c = conn.cursor()
	c.execute("""SELECT * from RSS""")
	results = c.fetchall()
	c.close()
	conn.close()
	return len(results)


def initDB(self):
	conn = sqlite3.connect(self.DB_file)
	c = conn.cursor()
	c.execute("""create table if not exists RSS (t text, website text, company text, title text  PRIMARY KEY, summary text)""")	
	conn.commit()
	c.close()
	conn.close()


if __name__ == "__main__":
	main()
