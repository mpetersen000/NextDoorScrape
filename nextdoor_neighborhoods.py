"""Parse nextdoor neighborhoods for a list of cities.
"""
import pandas as pd
import nextdoor_scraping as nds

class NextdoorNeighborhoods(nds.NextdoorScraping):
    """Class to scrape neighborhoods for a city on Nextdoor
    """

    def parse_neighborhoods(self, nextdoor_city, city_neighborhood_group, df_nextdoor_neighborhoods):
        """Parse neighborhoods from the div for each alphabet group
        """

        for div in city_neighborhood_group:
            #Find all the neighborhoods listed for a city on the page
            neighborhood_links = div.findAll('a')
            for nextdoor_neighborhood in neighborhood_links:
                self.logger.info("Parsing neighborhood: " + nextdoor_city.iat[0, nextdoor_city.columns.get_loc("City")] +  " " + nextdoor_neighborhood.string + " " +  nextdoor_neighborhood['href'])
                #Encode the name of the neighborhood
                neighborhood = str(nextdoor_neighborhood.string.encode('UTF-8'))
                neighborhood = neighborhood.lstrip('b')
                neighborhood = neighborhood.strip("'")
                #Append the new neighborhood to the dataframe
                df_nextdoor_neighborhoods = df_nextdoor_neighborhoods.append(
                {
                    "State": nextdoor_city.iat[0, nextdoor_city.columns.get_loc("State")],
                    "County": nextdoor_city.iat[0, nextdoor_city.columns.get_loc("County")],
                    "City": nextdoor_city.iat[0, nextdoor_city.columns.get_loc("City")],
                    "Neighborhood": neighborhood,
                    "Link": str(nextdoor_neighborhood['href'].strip())
                }, ignore_index=True)

        self.logger.info("Number of neighborhoods found: " + str(df_nextdoor_neighborhoods.shape[0]))

        return df_nextdoor_neighborhoods


    def parse_cities(self, df_cities, df_nextdoor_neighborhoods):
        """Parse each city
        """
        # Only scrape a subset of the cities
        for city in nds.CITIES:
            #Get the url for the page for each city
            city_lookup = df_cities[df_cities['City'] == city]["Link"].values[0]
            self.logger.info("City url: " + city_lookup)
            html_soup = super().make_request(city_lookup)

            #Parse all the neighborhoods from the page
            city_neighborhood_group = html_soup.find_all('div', class_='hood_group')
            nextdoor_city = df_cities[df_cities['City'] == city]
            df_nextdoor_neighborhoods = self.parse_neighborhoods(nextdoor_city, city_neighborhood_group, df_nextdoor_neighborhoods)

        return df_nextdoor_neighborhoods


    def scrape_neighborhoods(self):
        """Main loop to parse each city and each cities neighborhoods
        """
        #Create an empty dataframe with the correc columns
        df_nextdoor_neighborhoods = pd.DataFrame(columns=['State', 'County', 'City', 'Neighborhood', 'Link'])
        # Load the csv file containing all the cities we scraped from nextdoor
        df_cities = pd.read_csv(nds.CITIES_FILENAME)

        # Parse the city and neighborhoods
        df_nextdoor_neighborhoods = self.parse_cities(df_cities, df_nextdoor_neighborhoods)

        self.logger.info("Total number of neighborhoods found: " + str(df_nextdoor_neighborhoods.shape[0]))

        #Save the neighborhood data to a csv file for further processing
        df_nextdoor_neighborhoods.to_csv(nds.NEIGHBORHOOD_FILENAME, index=False)
        self.logger.info('Saved file: %s' % nds.NEIGHBORHOOD_FILENAME)



def main():
    nextdoor_neighborhoods = NextdoorNeighborhoods()
    nextdoor_neighborhoods.scrape_neighborhoods()


if __name__ == "__main__":
    main()
