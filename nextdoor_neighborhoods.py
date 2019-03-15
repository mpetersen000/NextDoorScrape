import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import pprint as pp

cities = ['San Jose','Santa Clara','Sunnyvale', 'Palo Alto','Mountain View',\
          'Cupertino','Milpitas','Los Gatos', 'Gilroy', 'Morgan Hill', 'Campbell',\
          'Los Altos','Saratoga', 'Stanford', 'Los Altos Hills',\
          'San Martin']

neighborhood_filename = 'nextdoor_california_neighborhoods.csv'
state_filename = 'nextdoor_california_cities.csv'

def find_all(a_str, sub):
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub) # use start += 1 to find overlapping matches

def make_request(url):
    from requests import get
    response = requests.get(url)
    #responseTxt = response.text.encode('UTF-8')
    #html_soup = BeautifulSoup(responseTxt, 'html.parser')
    html_soup = BeautifulSoup(response.text, 'html.parser')
    pp.pprint(type(html_soup))
    return html_soup, response

def parse_neighborhoods(city, city_neighborhood_group, df_nextdoor_neighborhoods):
    df_loc = 0

    for div in city_neighborhood_group:
        neighborhood_links = div.findAll('a')
        for a in neighborhood_links:
            print(city + " " + a.string + " " +  a['href'])
            df_nextdoor_neighborhoods.iloc[df_loc, df_nextdoor_neighborhoods.columns.get_loc("State")] = 'CA'
            df_nextdoor_neighborhoods.iloc[df_loc, df_nextdoor_neighborhoods.columns.get_loc("County")] = 'TBD'
            df_nextdoor_neighborhoods.iloc[df_loc, df_nextdoor_neighborhoods.columns.get_loc("City")] = city
            df_nextdoor_neighborhoods.iloc[df_loc, df_nextdoor_neighborhoods.columns.get_loc("Neighborhood")] = str(a.string.encode('UTF-8'))
            df_nextdoor_neighborhoods.iloc[df_loc, df_nextdoor_neighborhoods.columns.get_loc("Link")] = str(a['href'].strip())
            df_loc += 1

    df_nextdoor_neighborhoods.dropna(axis = 0, inplace=True)
    print(df_loc)
    return df_nextdoor_neighborhoods

def parse_cities(df_cities, df_nextdoor_neighborhoods):
    for city in cities:
        print(city)
        city_lookup = df_cities[df_cities['City'] == city]["Link"].values[0]
        print(city_lookup)
        html_soup, response = make_request(city_lookup)
        city_neighborhood_group = html_soup.find_all('div', class_ = 'hood_group')
        print(len(city_neighborhood_group))
        parse_neighborhoods(city, city_neighborhood_group, df_nextdoor_neighborhoods)

    return df_nextdoor_neighborhoods

def scrape_neighborhoods():
    df_nextdoor_neighborhoods = pd.DataFrame(index=range(20000), columns = ['State', 'County', 'City', 'Neighborhood', 'Link'])
    df_cities = pd.read_csv(state_filename)
    df_nextdoor_neighborhoods = parse_cities(df_cities, df_nextdoor_neighborhoods)

    df_nextdoor_neighborhoods.to_csv(neighborhood_filename, index=False)
    print('Saved file %s' % neighborhood_filename)
    pass

if __name__ == "__main__":
    print(scrape_neighborhoods())
