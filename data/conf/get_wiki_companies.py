from bs4 import BeautifulSoup
from time import sleep
from mwviews.api import PageviewsClient
import string
import requests
import urllib

headers={'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
base_html = 'https://en.wikipedia.org/wiki/Companies_listed_on_the_New_York_Stock_Exchange_(#)'
html_list = [base_html.replace('#', c) for c in list(string.ascii_uppercase)]


companies = {}
unique = set()
for html in html_list:
	page = requests.get(html, headers=headers).text
	soup = BeautifulSoup(page, 'lxml')

	company_list = soup.find(class_='mw-content-ltr')
	table = soup.find_all('table')[1].tbody
	count = 0 
	for link in table.findAll('tr')[1:]:
		if (link.find('td').find('a')):
			company = (link.findAll('a')[0]['href'].split('/')[2])
			ticker = (link.findAll('a')[1]['href'].split('/')[4].split(':')[1])
			if '?' not in company and company not in unique:
				companies[ticker] = company
				unique.update(company)

with open('companies.txt', 'w') as f:
	for ticker, company in companies.items():
		company_name  = urllib.parse.unquote(company)
		f.write( ticker + ' : ' + company_name + '\n')

