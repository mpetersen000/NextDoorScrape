import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import pprint as pp

state = 'CA'
state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
filename = 'nextdoor_california_cities.csv'

def make_request():
    from requests import get
    response = requests.get(state_url)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    pp.pprint(type(html_soup))
    return html_soup, response

def parse_state(html_soup, response):
    state_city_group = html_soup.find_all('div', class_ = 'hood_group')
    return state_city_group

def parse_cities(state_city_group, df_cities):
    df_loc = 0

    for div in state_city_group:
        city_links = div.findAll('a')
        for a in city_links:
            df_cities.iloc[df_loc, df_cities.columns.get_loc("State")] = 'CA'
            df_cities.iloc[df_loc, df_cities.columns.get_loc("City")] = a.string
            df_cities.iloc[df_loc, df_cities.columns.get_loc("Link")] = a['href'].strip()
            df_loc += 1

    df_cities.dropna(axis = 0, inplace=True)
    print(df_loc)
    return df_cities

def scrape_cities():
    html_soup, response = make_request()
    state_city_group = parse_state(html_soup, response)
    df_cities = pd.DataFrame(index=range(5000), columns = ['State', 'City', 'Link'])
    df_cities = parse_cities(state_city_group, df_cities)
    df_cities.to_csv(filename, index=False)
    print('Saved file %s' % filename)
    pass

if __name__ == "__main__":
    print(scrape_cities())
