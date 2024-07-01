import sqlite3
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

def connect_to_sql():
    """
    Establishes a connection to a SQLite3 database.
    It creates one if it doesn't exist yet.
    Also creates the table if it doesn't exist yet.
    Returns:
        tuple: A tuple (connection, cursor) if successful, otherwise (None, None).
    """
    try:
        conn = sqlite3.connect('.db')
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS LISTINGS (
                URL TEXT PRIMARY KEY,
                FLAG BOOLEAN NOT NULL
            )
        ''')
        conn.commit()

        print("Connection to database established")

        return conn, cursor
    
    except Exception as e:
        print(f"An error occurred in connect_to_sql: {e}")
        exit()

def insert_records(conn, cursor, links):
    """
    @@@
    """
    try:
        for link in links:
            cursor.execute("INSERT INTO LISTINGS (URL, FLAG) VALUES (?, 0)", [link])
        conn.commit()
    except Exception as e:
        print(f"An error occured in insert_records: {e}")
        exit()

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
        # We add all the links to the links array
        links = []
        divs = soup.find_all('div', class_='flex justify-between')
        for div in divs:
            link = div.find('a').get('href')
            links.append(link)
            print(link)
        return links

    except Exception as e:
        print(f"An error occurred in get_new_listings: {e}")
        exit()

def main():
    load_dotenv()

    links = get_new_listings()

    conn, cursor = connect_to_sql()

    insert_records(conn, cursor, links)

if __name__ == "__main__":
    main()