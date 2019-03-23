"""Parse nextdoor neighborhoods for a list of states.
"""
import nextdoor_scraping
import pandas as pd
import requests
from bs4 import BeautifulSoup


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
        for a_element in city_links:
            df_cities.iloc[df_loc, df_cities.columns.get_loc("State")] = 'CA'
            df_cities.iloc[df_loc, df_cities.columns.get_loc("City")] = a_element.string
            df_cities.iloc[df_loc, df_cities.columns.get_loc("Link")] = a_element['href'].strip()
            df_loc += 1

    df_cities.dropna(axis=0, inplace=True)
    print("Number of cities found: " + str(df_loc))
    return df_cities


def scrape_cities():
    """Parse pages for each state.
    """
    df_cities = pd.DataFrame(index=range(5000), columns=['State', 'City', 'Link'])
    df_counties = pd.read_csv(nextdoor_scraping.COUNTY_FILENAME)

    for state in nextdoor_scraping.STATES:
        state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
        html_soup = nextdoor_scraping.make_request(state_url)
        state_city_group = parse_state(html_soup)
        df_cities = parse_cities(state_city_group, df_cities)

    df_cities.to_csv(nextdoor_scraping.CITIES_FILENAME, index=False)
    print('Saved file: %s' % nextdoor_scraping.CITIES_FILENAME)


if __name__ == "__main__":
    print(scrape_cities())
