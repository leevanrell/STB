from subprocess import check_output
from time import gmtime, strftime, sleep
import time
import datetime
import feedparser
import configparser
import sqlite3
import sys
import re
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('./data/RSS.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(fmt)
fh.setFormatter(fmt)
logger.addHandler(ch)
logger.addHandler(fh)


Ticker_file = './data/ticker.txt'
DB_file = './data/rss.db'

Delay = 60 * 60

def main():
	Companies = open(Ticker_file).readlines()
	Companies = [line.strip() for line in Companies]
	#Columns: time, company, title
	conn = sqlite3.connect(DB_file)
	c = conn.cursor()
	c.execute("""create table if not exists Yahoo (t text, company text, title text  PRIMARY KEY, summary text)""")
	conn.commit()

	logger.debug('Looking up RSS on %s' % str(Companies))
	try:
		while True:
			getRSS(Companies, conn, c)
	except KeyboardInterrupt:
		logger.info('Detected KeyboardInterrupt')
		conn.commit()
		c.close()


def getRSS(Companies, conn, c):
	if datetime.datetime.today().weekday() < 5:
		for Company in Companies:
			feed = feedparser.parse('http://finance.yahoo.com/rss/headline?s=' + Company)
			feed_entries = feed.entries
			for entry in feed.entries:
				time = strftime(entry.published, gmtime())
				title = entry.title.replace('&apos;', '\'').replace('&quote;', '\'')
				raw = entry.summary.split('>')
				raw = [x for x in raw if 'http' not in x]
				summary = max(raw, key=len).replace('&apos;', '\'').replace('&quote;', '\'').replace('<p', '')
				if len(summary) > 25:
					row = [time, Company, title, summary]
					sql = '''INSERT OR IGNORE INTO Yahoo(t, company, title, summary) VALUES(?, ?, ?, ?)'''
					c.execute(sql, row)
					conn.commit()
				else:
					logger.debug('Summary too short: %s' % summary)
		c.execute('select COUNT(*) from Yahoo')
		count = c.fetchone()
		logger.info('Row count now at %s' % count)
	else:
		logger.debug('WEEKEND!')
	sleep(Delay)

if __name__ == "__main__":
	main()