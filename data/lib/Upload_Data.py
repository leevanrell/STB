#!/usr/bin/python3
"""

"""

from elasticsearch import Elasticsearch, helpers

try:
	from .Thread import Thread
except:
	from Thread import Thread

class Upload(Thread):

	def __init__(self, logger, q, server, bulk_size, threads):
		super().__init__('Upload', logger)
		self.BULK_SIZE = bulk_size
		self.q = q
		self.es = Elasticsearch([server])
		self.threads = threads


	async def run(self):
		self.logger.info('[%s] Starting thread' % self.Name)
		try:
			t = []
			while self.Running:
				doc = await self.q.get()
				t.append(doc)
				if len(t) >= self.BULK_SIZE:
					helpers.bulk(self.es, t)
					del t[:]
				self.threads_check()
			# while not self.q.empty():
			# 	t.append(self.q.get())
			# 	if len(t) >= self.BULK_SIZE:
			# 		helpers.bulk(self.es, t)
			# 		del t[:]
			helpers.bulk(self.es, t)
			self.Fin = True
		except Exception as e:
			self.logger.error('[%s] Error encountered %s' % (self.Name, e))


	def threads_check(self):
		for thread in self.threads:
			if not thread.Fin:
				return
		self.Running = False
		self.logger.info('[%s] all threads done, finishing.' % self.Name)
		return


if __name__ == "__main__":
	API_key = '2RPX5G5M7XOXMDJU'

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	fmt = logging.Formatter('%(asctime)s - %(threadName)-11s -  %(levelname)s - %(message)s')
	ch.setFormatter(fmt)
	logger.addHandler(ch)

	import asyncio
	q = asyncio.Queue()
	q.put({'_index': 'stock', '_type': 'data_basic', '_source': {'upload_date': '04/21/2019/', 'symbol': 'YI', 'name': '111, Inc.', 'price_close': 7.7, 'IPO_year': '2018', 'sector': 'Health Care', 'industry': 'Medical/Nursing Services', 'mkt_cap': 627890000.0, 'avg_volume': '28,092', 'next_earnings': '6/6/19'}})
	server = {'host': 'localhost', 'port': 9200}
	s = Upload(logger, server, 100, q)
	s.run('3')