"""Re-usable functions for scraping nextdoor neighborhoods.
"""
import re
import logging
import logging.config
import requests
from bs4 import BeautifulSoup

STATES = ['CA']

CITIES = ['San Jose', 'Santa Clara', 'Sunnyvale', 'Palo Alto',\
'Mountain View', 'Cupertino', 'Milpitas', 'Los Gatos', 'Gilroy',\
'Morgan Hill', 'Campbell', 'Los Altos', 'Saratoga', 'Stanford',\
'Los Altos Hills', 'San Martin']

#CITIES = ['San Martin']

NEIGHBORHOOD_EXT_FILENAME = 'nextdoor_california_neighborhoods_ext.csv'
NEIGHBORHOOD_FILENAME = 'nextdoor_california_neighborhoods.csv'
STATES_FILENAME = 'us-state-ansi-fips.csv'
CITIES_FILENAME = 'nextdoor_california_cities.csv'
COUNTIES_FILENAME = 'uscitiesv1.4.csv'
GEOJSON_FILENAME = 'nextdoor_neighborhoods.geojson'
EDGELIST_FILENAME = 'nextdoor_neighborhoods.edgelist'


class NextdoorScraping:
    """Baseclass for scraping various data from NextDoor
    """

    def __init__(self):
        self.logger = create_logger()

    def make_request(self, url):
        """Make a request to retrieve html from url
        """
        html_soup = ""
        try:
            url = clean_string(url)
            response = requests.get(url)
            response_text = response.text.encode('UTF-8')
            html_soup = BeautifulSoup(response_text, 'html.parser')
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("Error making request for: " + url)

        return html_soup


def clean_string(a_str):
    """Remove all the non ascii characters from a string.
    """
    re.sub(r'[^\x00-\x7f]', r'', a_str)
    return a_str


def find_all(a_str, name):
    """Find all the values in a dictionary string after name
    """
    start = 0
    while True:
        start = a_str.find(name, start)
        if start == -1:
            return
        yield start
        start += len(name)


def find_county_for_city(df_counties, city_fullname, state_abbr):
    """Find the county a city is in
    """
    county_fullname = ""

    #Reduce the counties to just the state since cities do not have unique names
    df_counties = df_counties.loc[df_counties["state_id"] == state_abbr]
    county = df_counties.loc[df_counties['city_ascii'].str.lower() == city_fullname.lower()]

    #Ensure we found just one county
    if len(county) == 1:
        county_fullname = county.iat[0, county.columns.get_loc("county_name")]

    return county_fullname

def create_logger():
    """Create a logger
    """
    # Create logger
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('nextdoor_scraping')

    return logger
