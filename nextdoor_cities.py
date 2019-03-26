"""Parse nextdoor neighborhoods for a list of states.
"""
import nextdoor_scraping
import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup


def parse_state(html_soup):
    """Parse div that contains all the cities for a state
    """
    state_city_group = html_soup.find_all('div', class_='hood_group')
    return state_city_group


def update_county(df_cities, df_counties, state_fullname):
    """Update the name of the county for each city
    """
    for index, city in df_cities.iterrows():
        county = nextdoor_scraping.find_county_for_city(df_counties, city["City"], state_fullname)
        df_cities.iat[index, df_cities.columns.get_loc("County")] = county

    return df_cities


def parse_cities(state_city_group, df_cities, state):
    """Parse links that contains each city for a state
    """
    global logger

    for div in state_city_group:
        city_links = div.findAll('a')
        #Append each city to the dataframe
        for a_element in city_links:
            df_cities = df_cities.append(
            {
            "State": state,
            "County": "",
            "City": a_element.string,
            "Link": a_element['href'].strip(),
            }, ignore_index=True)

    #Log the number of rows we added for the cities we scraped
    logger.info("Number of cities found: " + str(df_cities.shape[0]))
    return df_cities


def scrape_cities(logger):
    """Parse pages for each state.
    """
    df_cities = pd.DataFrame(columns=['State', 'County', 'City', 'Link'])

    # Read in list of counties for the US
    df_counties = pd.read_csv(nextdoor_scraping.COUNTIES_FILENAME)

    for state in nextdoor_scraping.STATES:
        state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
        html_soup = nextdoor_scraping.make_request(state_url)
        #Grab the list of cities from the page for the state
        state_city_group = parse_state(html_soup)

        # Parse the info for the state and update the county name
        df_cities = parse_cities(state_city_group, df_cities, state)
        df_cities = update_county(df_cities, df_counties, state)

    df_cities.to_csv(nextdoor_scraping.CITIES_FILENAME, index=False)
    logger.info('Saved file: %s' % nextdoor_scraping.CITIES_FILENAME)
    return "Success"


if __name__ == "__main__":
    logger = nextdoor_scraping.create_logger()
    logger.info(scrape_cities(logger))
