"""Parse nextdoor neighborhoods for a list of states.
"""
import pprint as pp
import pandas as pd
import requests
from bs4 import BeautifulSoup

STATES = ['CA']
CITIES_FILENAME = 'nextdoor_california_cities.csv'


def make_request(url):
    """Make a request to retrieve html from url
    """
    response = requests.get(url)
    response_text = response.text.encode('UTF-8')
    html_soup = BeautifulSoup(response_text, 'html.parser')
    return html_soup


def parse_state(html_soup):
    """Parse div that contains all the cities for a state
    """
    state_city_group = html_soup.find_all('div', class_='hood_group')
    return state_city_group


def parse_cities(state_city_group, df_cities):
    """Parse links that contains each city for a state
    """
    df_loc = 0

    for div in state_city_group:
        city_links = div.findAll('a')
        for a in city_links:
            df_cities.iloc[df_loc, df_cities.columns.get_loc("State")] = 'CA'
            df_cities.iloc[df_loc, df_cities.columns.get_loc("City")] = a.string
            df_cities.iloc[df_loc, df_cities.columns.get_loc("Link")] = a['href'].strip()
            df_loc += 1

    df_cities.dropna(axis = 0, inplace=True)
    print("Number of cities found: " + str(df_loc))
    return df_cities


def scrape_cities():
    """Parse pages for each state.
    """
    df_cities = pd.DataFrame(index=range(5000), columns=['State', 'City', 'Link'])
    for state in STATES:
        state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
        html_soup = make_request(state_url)
        state_city_group = parse_state(html_soup)
        df_cities = parse_cities(state_city_group, df_cities)

    df_cities.to_csv(CITIES_FILENAME, index=False)
    print('Saved file: %s' % CITIES_FILENAME)


if __name__ == "__main__":
    print(scrape_cities())
