import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

load_dotenv()

api_key = os.getenv('scraper_api_key')
url = 'https://www.funda.nl/zoeken/koop?selected_area=%5B%22amstelveen,5km%22%5D&price=%22450000-675000%22&object_type=%5B%22apartment%22%5D&floor_area=%2280-%22&rooms=%224-%22&search_result=1'
country_code = 'eu'
device_type = 'desktop'
url_scraper = 'https://api.scraperapi.com/'

payload = {
    'api_key': api_key,
    'url': url,
    'country_code': country_code,
    device_type: device_type
}

response = requests.get(url_scraper, params=payload)

soup = BeautifulSoup(response.content, 'html.parser')

links = soup.find_all('a', href=lambda href: href and href.startswith('https://www.funda.nl/detail/koop/'))

for link in links:
    print(link['href'])