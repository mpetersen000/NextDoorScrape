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

def update_county(df_cities, df_counties, state):

    for index, city in df_cities.iterrows():
        #TODO
        df_counties = df_counties.loc[df_counties["state_name"] == 'California']
        county = df_counties.loc[df_counties['city_ascii'].str.lower() == city["City"].lower()]
        if len(county) == 1:
            city["County"] = county.iat[0, county.columns.get_loc("county_name")]

    return df_cities


def parse_cities(state_city_group, df_cities, state):
    """Parse links that contains each city for a state
    """
    df_loc = 0

    for div in state_city_group:
        city_links = div.findAll('a')
        for a_element in city_links:
            df_cities = df_cities.append(
            {
            "State": state,
            "County": "",
            "City": a_element.string,
            "Link": a_element['href'].strip(),
            }, ignore_index=True)
            df_loc += 1

    print("Number of cities found: " + str(df_loc))
    return df_cities


def scrape_cities():
    """Parse pages for each state.
    """
    df_cities = pd.DataFrame(columns=['State', 'County', 'City', 'Link'])
    df_counties = pd.read_csv(nextdoor_scraping.COUNTY_FILENAME)

    #TODO read in state and state abbreviations from a file

    for state in nextdoor_scraping.STATES:
        state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
        html_soup = nextdoor_scraping.make_request(state_url)
        state_city_group = parse_state(html_soup)
        df_cities = parse_cities(state_city_group, df_cities, state)

        df_cities = update_county(df_cities, df_counties, state)

    df_cities.to_csv(nextdoor_scraping.CITIES_FILENAME, index=False)
    print('Saved file: %s' % nextdoor_scraping.CITIES_FILENAME)


if __name__ == "__main__":
    print(scrape_cities())
