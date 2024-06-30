import mysql.connector
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

def connect_to_sql():
    """
    Establishes a connection to a MySQL database using credentials from a .env file.
    Returns:
        tuple: A tuple (connection, cursor) if successful, otherwise (None, None).
    """
    try:
        # Make database connection
        connection = mysql.connector.connect(
            host = os.getenv('host'),
            user = os.getenv('user'),
            passwd = os.getenv('passwd'),
            database = os.getenv('database')
        )

        # Make cursor with which we can query
        cursor = connection.cursor()

        return connection, cursor
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

def get_new_listings():
    """
    @@@
    """
    try:
        # Retrieving scraper API key and setting up parameters for the HTML query
        # Scraper API is used to prevent from being identified as a bot
        api_key = os.getenv('scraper_api_key')
        url = 'https://www.funda.nl/zoeken/koop?selected_area=%5B%22amstelveen,5km%22%5D&price=%22450000-675000%22&object_type=%5B%22apartment%22%5D&floor_area=%2280-%22&rooms=%224-%22&search_result=1'
        country_code = 'eu'
        device_type = 'desktop'
        url_scraper = 'https://api.scraperapi.com/'

        # Making a payload with our request variables
        payload = {
            'api_key': api_key,
            'url': url,
            'country_code': country_code,
            device_type: device_type
        }

        # Executing our HTML request, and parsing it
        response = requests.get(url_scraper, params=payload)
        soup = BeautifulSoup(response.content, 'html.parser')

        # We find all the divs with class flex justify-between
        # Then we loop through all those divs to find the a tag with the href
        divs = soup.find_all('div', class_='flex justify-between')
        for div in divs:
            link = div.find('a').get('href')
            print(link)

    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    load_dotenv()

    get_new_listings()

if __name__ == "__main__":
    main()