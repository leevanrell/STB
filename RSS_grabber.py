from subprocess import check_output
from time import gmtime, strftime, sleep
import time
import datetime
import feedparser
import configparser
import sqlite3
import sys
import re

Ticker_file = './data/ticker.txt'
DB_file = './data/rss.db'

Delay = 60 * 60

Companies = open(Ticker_file).readlines()
Companies = [line.strip() for line in Companies]
#Columns: time, company, title
conn = sqlite3.connect(DB_file)
c = conn.cursor()
c.execute("""create table if not exists Yahoo (t text, company text, title text  PRIMARY KEY, summary text)""")
conn.commit()

try:
	while True:
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
		sleep(Delay)
except KeyboardInterrupt:
	conn.commit()
	c.close()