#!/usr/bin/python3
"""

"""

#elasticsearch config
server = {'host': 'localhost', 'port': 9200}
index = 'stock' 


#initial stock lists
NASDAQ_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
NYSE_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'


es_mappings = {
		"settings": {
			"number_of_shards" :   5,
			"number_of_replicas" : 1
		},
		"mappings": {
			"data_basic": {
				"properties": {
					"symbol": {"type": "keyword"},
					"name": {"type": "text"},		
					"price_close": {"type": "float"},
					"mkt_cap": { "type": "integer"},
					"avg_volume": {"type": "integer"},
					"sector": {"type": "text"},	
					"industry": {"type": "text"},	
					'upload_date': { 
						"type": "date",
						"format": "MM/dd/yyyy"
					},	
					"IPO_year": { 
						"type": "date",
						"format": "yyyy"
					},	
					"next_earnings" : { 
						"type": "date",
						"format": "MM/dd/yyyy"
					},
				}
			},
			"data_stats": {
				"properties": { 
					"symbol": { "type": "keyword"},
					"price_last": { "type": "float"},
					"date": { 
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
					"price_open": { "type": "float"},
					"price_high": { "type": "float"},
					"price_low": { "type": "float"},
					"price_close": {"type": "float"},
					"vol": { "type": "integer"},                
					"EMA20": {"type": "float"},
					"SMA20": {"type": "float"},
					"SMA50": {"type": "float"},                
					"SMA200": {"type": "float"},
					"RSI": {"type": "float"},
					"MACD": {"type": "float"}
				}
			},
			"data_wiki": {
				"properties": {
					"symbol": {"type": "keyword"},
					"company_name": {"type": "text"},
					"wiki_views": {"type": "integer"},
					"date": {
						"type": "date",
						"format": "MM/dd/yyyy" # change date to MM/dd/yyyy
					},
				}
			},
			"data_rss": {
				"properties": {
					"symbol": {"type": "keyword"},
					"article_title": {"type" : "text"},
					"time": {
						"type": "date",
						"format": "MM/dd/yyyy HH:MM:SS" # change date to MM/dd/yyyy
					},
				}
			}
		}
	}
