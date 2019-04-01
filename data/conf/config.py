#!/usr/bin/python3
"""

"""


#api keys
Quandl_api_key = 'Tj6uPSoMETLVpFER3nsX'

#elasticsearch config
server = {'host': 'localhost', 'port': 9200}
db_name = 'stock' 


#initial stock lists
NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
NYSE_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'


Proc_Count = 16