"""Parse nextdoor neighborhoods for a list of cities.
"""
import pandas as pd
import nextdoor_scraping

def parse_neighborhoods(city, city_neighborhood_group, df_nextdoor_neighborhoods, df_loc):
    """Parse neighborhoods from the div for each alphabet group
    """

    for div in city_neighborhood_group:
        neighborhood_links = div.findAll('a')
        for nextdoor_neighborhood in neighborhood_links:
            print("Parsing neighborhood: " + city +  " " + nextdoor_neighborhood.string + " " +  nextdoor_neighborhood['href'])
            df_nextdoor_neighborhoods.iloc[df_loc,\
            df_nextdoor_neighborhoods.columns.get_loc("State")] = 'CA'
            #TODO
            df_nextdoor_neighborhoods.iloc[df_loc,\
            df_nextdoor_neighborhoods.columns.get_loc("County")] = 'Santa Clara'
            df_nextdoor_neighborhoods.iloc[df_loc,\
            df_nextdoor_neighborhoods.columns.get_loc("City")] = city
            neighborhood = str(nextdoor_neighborhood.string.encode('UTF-8'))
            neighborhood = neighborhood.lstrip('b')
            neighborhood = neighborhood.strip("'")
            df_nextdoor_neighborhoods.iloc[df_loc,\
            df_nextdoor_neighborhoods.columns.get_loc("Neighborhood")] = neighborhood
            df_nextdoor_neighborhoods.iloc[df_loc,\
            df_nextdoor_neighborhoods.columns.get_loc("Link")] = str(nextdoor_neighborhood['href'].strip())
            df_loc += 1

    print("Number of neighborhoods found: " + str(df_loc))

    return df_nextdoor_neighborhoods, df_loc


def parse_cities(df_cities, df_nextdoor_neighborhoods):
    """Parse each city
    """
    df_loc = 0
    for city in nextdoor_scraping.CITIES:
        city_lookup = df_cities[df_cities['City'] == city]["Link"].values[0]
        print("City url: " + city_lookup)
        html_soup = nextdoor_scraping.make_request(city_lookup)
        city_neighborhood_group = html_soup.find_all('div', class_='hood_group')
        df_nextdoor_neighborhoods, df_loc = parse_neighborhoods(city, city_neighborhood_group, df_nextdoor_neighborhoods, df_loc)

    df_nextdoor_neighborhoods.dropna(axis=0, inplace=True)
    return df_nextdoor_neighborhoods


def scrape_neighborhoods():
    """Main loop to parse each city and each cities neighborhoods
    """
    df_nextdoor_neighborhoods = pd.DataFrame(index=range(20000),\
    columns=['State', 'County', 'City', 'Neighborhood', 'Link'])
    df_cities = pd.read_csv(nextdoor_scraping.STATE_FILENAME)
    df_nextdoor_neighborhoods = parse_cities(df_cities, df_nextdoor_neighborhoods)

    print("Total number of neighborhoods found: " + str(df_nextdoor_neighborhoods.shape[0]))

    df_nextdoor_neighborhoods.to_csv(nextdoor_scraping.NEIGHBORHOOD_FILENAME, index=False)
    print('Saved file: %s' % nextdoor_scraping.NEIGHBORHOOD_FILENAME)
    return "Success"


if __name__ == "__main__":
    print(scrape_neighborhoods())
