#!/usr/bin/python3
"""
	Verbose display for running theads
	Prints status of running threads, Number of Rows in Databases
"""

from time import sleep
import sys
import sqlite3
import datetime
import os
import time

from data.Thread import Thread

sys.path.append('../')


class Screen(Thread):

	def __init__(self, logger, VERSION, Threads):
		super().__init__('Screen', logger)
		self.VERSION = VERSION
		self.Threads = Threads
		self.Refresh = 15

	def run(self, loop):
		start_time = datetime.datetime.now()
		while self.Running:
			self.displayStatus(start_time)
			sleep(self.Refresh)
			os.system('clear')
		self.waitforThreads()
		print('Fin.')
		self.Fin = True

	def displayStatus(self, start_time):
		print("Started STB v%s at %s " % (self.VERSION,
		 time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.localtime())))
		print("Duration: {}".format(datetime.datetime.now() - start_time))
		for thread in self.Threads:
			print("Current %s Entry Count: %s" % (thread.name, self.getRowCount(self.Yahoo.DB_file, 'RSS')))

	def waitforThreads(self):
		wait = True
		while wait:
			for thread in self.Threads:
				if not thread.Fin:
					print('Waiting on %s thread to finish. ' % thread.Name)
				else:
					pass
			sleep(self.Refresh)
			os.system('clear')

	def getRowCount(self, DB_file, Table):
		try:
			conn = sqlite3.connect(DB_file, timeout=7)
			c = conn.cursor()
			c.execute("""SELECT * from %s""" % Table)
			results = c.fetchall()
			c.close()
			conn.close()
			return len(results)
		except Exception as e:
			return e
