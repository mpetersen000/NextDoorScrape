"""Parse nextdoor neighborhoods for a list of states.
"""
import pandas as pd
from bs4 import BeautifulSoup
import nextdoor_scraping as nds

class NextdoorCities(nds.NextdoorScraping):
    """Class to scrape cities for a state on Nextdoor
    """

    def __init__(self):
        super().__init__()
        self.logger.info("Create NextdoorCities")


    def parse_state(self, html_soup):
        """Parse div that contains all the cities for a state
        """
        state_city_group = html_soup.find_all('div', class_='hood_group')
        return state_city_group


    def update_county(self, df_cities, df_counties, state_fullname):
        """Update the name of the county for each city
        """
        for index, city in df_cities.iterrows():
            county = nds.find_county_for_city(df_counties, city["City"], state_fullname)
            df_cities.iat[index, df_cities.columns.get_loc("County")] = county

        self.logger.info("Updating County for each City")
        return df_cities


    def parse_cities(self, state_city_group, df_cities, state):
        """Parse links that contains each city for a state
        """

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
        self.logger.info("Number of cities found: " + str(df_cities.shape[0]))
        return df_cities


    def scrape_cities(self):
        """Parse pages for each state.
        """
        df_cities = pd.DataFrame(columns=['State', 'County', 'City', 'Link'])

        # Read in list of counties for the US
        df_counties = pd.read_csv(nds.COUNTIES_FILENAME)

        for state in nds.STATES:
            state_url = 'https://nextdoor.com/find-neighborhood/' + state + '/'
            html_soup = super().make_request(state_url)

            #Grab the list of cities from the page for the state
            state_city_group = self.parse_state(html_soup)

            # Parse the info for the state and update the county name
            df_cities = self.parse_cities(state_city_group, df_cities, state)
            df_cities = self.update_county(df_cities, df_counties, state)

        df_cities.to_csv(nds.CITIES_FILENAME, index=False)
        self.logger.info('Saved file: %s' % nds.CITIES_FILENAME)
        return "Success"

def main():
    nextdoor_cities = NextdoorCities()
    nextdoor_cities.scrape_cities()


if __name__ == "__main__":
    main()
