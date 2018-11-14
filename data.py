#!/usr/bin/python3
from subprocess import check_output
from time import gmtime, strftime, sleep
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
import time
import datetime
import feedparser
import configparser
import sqlite3
import sys
import re
import logging
import asyncio

from lib.SQL_manager import SQL
from lib.RSS_Yahoo import RSS_Yahoo
from lib.RSS_NASDAQ import RSS_NASDAQ

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

RSS_TIMEOUT = 1
SQL_TIMEOUT = 1
Processes_Count = 3
Fin_TIMEOUT = 3


def main():
	Companies = [line.strip() for line in open(Ticker_file).readlines()]
	logger.debug('Looking up RSS on %s' % str(Companies))

	q = asyncio.Queue()
	conn = initDB()

	Yahoo = RSS_Yahoo(logger, q, RSS_TIMEOUT, 'Yahoo', 'http://finance.yahoo.com/rss/headline?s=', Companies)
	NASDAQ = RSS_NASDAQ(logger, q, RSS_TIMEOUT, 'NASDAQ', 'http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=', Companies)
	Sql = SQL(logger, q, SQL_TIMEOUT, conn)

	logger.info("Starting %s Threads" % Processes_Count)
	executor = ProcessPoolExecutor(Processes_Count)
	loop = asyncio.get_event_loop()
	future_Yahoo = loop.run_in_executor(None, Yahoo.run, loop)
	future_NASDAQ = loop.run_in_executor(None, NASDAQ.run, loop)
	future_Sql = loop.run_in_executor(None, Sql.run, loop)
	#loop.run_until_complete(Sql.run(loop))
	try:
		loop.run_forever()
	except KeyboardInterrupt:
		print('\r')
		logger.info('Detected KeyboardInterrupt')
		Yahoo.running = False
		NASDAQ.running = False
		Sql.running = False
	sleep(Fin_TIMEOUT)
	future_Yahoo.cancel()
	future_NASDAQ.cancel()
	future_Sql.cancel()
	conn.close()
	loop.close()
	logger.info('Fin.')


def initDB():
	conn = sqlite3.connect(DB_file)
	c = conn.cursor()
	c.execute("""create table if not exists RSS (t text, website text, company text, title text  PRIMARY KEY, summary text)""")	
	conn.commit()
	c.close()
	return conn


if __name__ == "__main__":
	main()
