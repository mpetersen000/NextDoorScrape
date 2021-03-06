"""Parse extended data for nextdoor neighborhoods.
"""
import re
import json
import pandas as pd
import networkx as nx
import nextdoor_scraping as nds

class NextdoorNeighborhoodsExt(nds.NextdoorScraping):
    """Class to scrape extended information for neighborhoods on Nextdoor
    """

    def update_neighborhood_df(self, df_nextdoor_neighborhoods):
        """Add columns for the new neighborhood fields
        """
        df_nextdoor_neighborhoods["Nextdoor ID"] = 0
        df_nextdoor_neighborhoods["Geometry"] = ""
        df_nextdoor_neighborhoods["Interests"] = ""
        df_nextdoor_neighborhoods["Percentage of Homeowners"] = 0
        df_nextdoor_neighborhoods["Number of Residents"] = 0
        df_nextdoor_neighborhoods["Average Age"] = 0
        df_nextdoor_neighborhoods["Attributes"] = ""
        df_nextdoor_neighborhoods["Nearby Neighborhoods"] = ""

        df_nextdoor_neighborhoods["Geometry"].fillna("", inplace=True)
        df_nextdoor_neighborhoods["Interests"].fillna("", inplace=True)
        df_nextdoor_neighborhoods["Attributes"].fillna("", inplace=True)
        df_nextdoor_neighborhoods["Nearby Neighborhoods"].fillna("", inplace=True)

        return df_nextdoor_neighborhoods


    def update_nearby_neighborhood_info(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """Iterate over each neighborhood and update the URL and ID from the \
        information in the neighborhood map variable.\
        """
        try:
            #Grab the content from the page listing all the nearby neighborhoods
            neighborhood_map = neighborhood_info[neighborhood_info.index("neighborhoodMapOptions:{"): -1]
            neighborhood_map = neighborhood_map.strip()
            #Determine the beginning and ending indexes for the neighborhood page url, the neighborhood name, and nextdoor id
            beg_url_indexes = list(nds.find_all(neighborhood_map, "\"page_url\": \""))
            end_url_indexes = list(nds.find_all(neighborhood_map, "\", \"short_name\": "))
            beg_shortname_indexes = list(nds.find_all(neighborhood_map, "\"short_name\": \""))
            end_shortname_indexes = list(nds.find_all(neighborhood_map, "\", \"stroke_color\": "))
            beg_id_indexes = list(nds.find_all(neighborhood_map, "\"id\": "))
            end_id_indexes = list(nds.find_all(neighborhood_map, ", \"geometry\": \"{"))

            #Ensure we have the same number of beginning and ending indexes
            if ((len(beg_url_indexes) == len(end_url_indexes)) and\
            (len(beg_shortname_indexes) == len(end_shortname_indexes)) and\
            (len(beg_id_indexes) == len(end_id_indexes))):
                for beg_short, end_short, beg_url, end_url, beg_id, end_id in\
                zip(beg_shortname_indexes, end_shortname_indexes,\
                beg_url_indexes, end_url_indexes,\
                beg_id_indexes, end_id_indexes):

                    #Parse the neighborhood name
                    shortname = neighborhood_map[beg_short + len("\"short_name\": \"") : end_short]
                    shortname = re.sub(r'\s+', ' ', shortname).strip()

                    #Look up the existing entry to update
                    existing_entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == shortname]
                    if len(existing_entry) == 1:
                        index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Neighborhood'] == shortname][0])

                        #If the url is not set then set it
                        if len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")]) == 0:
                            page_url = neighborhood_map[beg_url + len("\"page_url\": \"") : end_url]
                            self.logger.info("Updating Page URL: " + page_url)
                            if len(page_url) > 0:
                                df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")] = page_url

                        #If the id is not set then set it
                        if df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0:
                            neighborhood_id = neighborhood_map[beg_id + len("\"id\": ") : end_id]
                            self.logger.info("Updating Nextdoor ID: " + neighborhood_id)
                            df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(neighborhood_id)

                        #Output the updated entry
                        self.logger.info(df_nextdoor_neighborhoods.iloc[index])

        except Exception as e:
            self.logger.error(e)
            self.logger.error("Error update_nearby_neighborhood_info for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods


    def add_nearby_neighborhoods(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """Add neighborhoods that we found as part of scraping. Some neighborhoods\
        are only listed in the nearby neighborhood sections and not on the main\
        page of neighborhoods for a city.
        """
        try:
            #Create an empty list of nearby neighborhoods
            nearby_neighborhood_list = []

            #Get the nearby neighborhoods from the page
            nearby_neighborhoods =\
            neighborhood_info[neighborhood_info.index('nearbyNeighborhoods: '): neighborhood_info.index('neighborhoodGeometriesJSON:')]
            nearby_neighborhoods = nearby_neighborhoods.strip()
            beg_index = len('nearbyNeighborhoods: ')
            nearby_neighborhoods = nearby_neighborhoods[beg_index: -1]

            nearby_neighborhood_json = json.loads(nearby_neighborhoods)

            for nearby_neighborhood in nearby_neighborhood_json:
                #Append the neighborhood name to the list of nearby neighborhoods
                nearby_neighborhood_list.append(nearby_neighborhood['shortName'])

                if nearby_neighborhood['city'] in nds.CITIES:
                    nearby_neighborhood['shortName'] = re.sub(r'\s+', ' ', nearby_neighborhood['shortName']).strip()

                    #Find the entry with the same name and city since neighborhood #names are not unique
                    existing_entry = df_nextdoor_neighborhoods.loc[(df_nextdoor_neighborhoods['Neighborhood'] == nearby_neighborhood['shortName']) & (df_nextdoor_neighborhoods['City'] == nearby_neighborhood['city'])]
                    if len(existing_entry) == 0:
                        self.logger.info("Adding Neighborhood: ", nearby_neighborhood)
                        df_nextdoor_neighborhoods = df_nextdoor_neighborhoods.append(
                        {"State": nearby_neighborhood['state'],

                        #Assume this neighborhood is in the same county as the one we are processing
                         "County":  df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("County")],
                         "City": nearby_neighborhood['city'],
                         "Neighborhood": nearby_neighborhood['shortName'],
                         "Link": "",
                         "Nextdoor ID": 0,
                         "Geometry": "",
                         "Interests": "",
                         "Percentage of Homeowners": 0,
                         "Number of Residents": 0,
                         "Average Age": 0,
                         "Attributes": "",
                         "Nearby Neighborhoods": ""
                        }, ignore_index=True)

            #Update the current entry with list of nearby neighborhoods
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Nearby Neighborhoods")] = str(nearby_neighborhood_list)

        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("Error add_nearby_neighborhoods for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods

    def update_current_page_neighborhood_id(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """ Find the ID of the neighborhood the page is for
        """
        try:
            neighborhood_map = neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
            neighborhood_map = neighborhood_map.strip()
            #Determine the beginning and ending indexes for the neighborhood nextdoor id

            beg_index = list(nds.find_all(neighborhood_map, "hoodId: "))
            assert len(beg_index) == 1
            end_index = beg_index[0] + len("hoodId: ") + 10
            hood_id = neighborhood_map[beg_index[0] + len("hoodId: "): end_index]
            hood_id = hood_id.strip()
            hood_id = re.findall(r'^([\s\d]+)$', hood_id)
            assert len(hood_id) == 1
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(hood_id[0])

        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("Error update_current_page_neighborhood_id for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods


    def update_nearby_neighborhood_features(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods, neighborhood_g):
        """ Update the geometry of each neighborhood.
        """
        try:
            #Get the json for the neighborhood geometries from the page
            neighborhood_geometries = neighborhood_info[neighborhood_info.index('neighborhoodGeometriesJSON: '): neighborhood_info.index('neighborhoodMapOptions:{')]
            neighborhood_geometries = neighborhood_geometries.strip()
            beg_index = len('neighborhood_geometriesJSON: ') - 1
            neighborhood_geometries = neighborhood_geometries[beg_index: -1]
            neighborhood_json = json.loads(neighborhood_geometries)

            # Iterate over all the features
            for feature in neighborhood_json["features"]:
                props = feature["properties"]
                props["hood_name"] = re.sub(r'\s+', ' ', props['hood_name']).strip()
                self.logger.info("Updating features for: %s, ID: %s \n" % (props["hood_name"], props["hood_id"]))

                #Add the edges of neighborhood ids and names to the networkx graph of nearby neighborhoods
                neighborhood_g.add_edge(
                df_nextdoor_neighborhoods.iloc[current_row_index]["Nextdoor ID"], props["hood_id"], neighborhood_name1=df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"],
                neighborhood_name2=props["hood_name"])

                #Find the entry to update the geometry and properties for
                existing_entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == props["hood_name"]]
                if len(existing_entry) == 1:
                    index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Nextdoor ID'] == props["hood_id"]][0])

                    #Save the feature and properties for each neighborhood without the featurecollection
                    if len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Geometry")]) == 0:
                        #Add features to the properties.  This enables joins.
                        feature['properties']['State'] = df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("State")]
                        feature['properties']['City'] = df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("City")]
                        feature['properties']['County'] = df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("County")]
                        feature['properties']['Neighborhood'] = props["hood_name"]
                        feature['properties']['Nextdoor ID'] = props["hood_id"]

                        #Removed unused and renamed properties
                        del feature['properties']['hood_name']
                        del feature['properties']['hood_id']
                        del feature['properties']['fill']
                        del feature['properties']['stroke']
                        del feature['properties']['fill-opacity']

                        #Update the entry with the feature and properties
                        df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Geometry")] = json.dumps(feature)

                        #Update the nextdoor ID if it isn't already set.  This can occur when we add a new neighborhood as part of this process.  Not all the nieghborhoods for a city are listed on the page for the city
                        if (df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0):
                            df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = props["hood_id"]

                    self.logger.info(df_nextdoor_neighborhoods.iloc[index])

        except Exception as e:
            self.logger.error(e)
            self.logger.error("Error update_nearby_neighborhood_features for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods, neighborhood_g


    def get_current_page_neighborhood_info(self, current_row_index, df_nextdoor_neighborhoods):
        """ Find the variable on the page with all the neighborhood details.  It is in script[5]
        """
        NEIGHBORHOOD_INFO_SCRIPT_INDEX = 5
        neighborhood_info = ""
        try:
            if len(df_nextdoor_neighborhoods.iloc[current_row_index]["Link"]) > 0:
                neighborhood_lookup = df_nextdoor_neighborhoods.iloc[current_row_index]["Link"]
                html_soup = super().make_request(neighborhood_lookup)
                if len(html_soup) > 0:
                    scripts = html_soup.find_all('script')
                    neighborhood_info = scripts[NEIGHBORHOOD_INFO_SCRIPT_INDEX].string.strip()
                    neighborhood_info = neighborhood_info[neighborhood_info.index('['):-1]

        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("Error in  get_current_page_neighborhood_info: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return neighborhood_info


    def update_neighborhood_interests(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """ Update the neighborhood with the top 10 interests.
        """
        try:
            beg_index = neighborhood_info.index('interests: [')
            end_index = neighborhood_info.index('iosUrl: "https://')
            if ((beg_index > 0) and (end_index > 0)):
                interests = neighborhood_info[beg_index: end_index]
                interests = interests[interests.index('[')+1: interests.index(']')]
                interests = interests.replace('"', "")
                interest_list = interests.split(", ")
                self.logger.info("Interests: " + str(interest_list))
                df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Interests")] = interest_list
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("No interests found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods


    def update_neighborhood_attributes(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """ Update the neighborhood with the top 10 attributes.
        """
        try:
            beg_index = neighborhood_info.index('attributes: [')
            end_index = neighborhood_info.index('census:')
            if ((beg_index > 0) and (end_index > 0)):
                attributes = neighborhood_info[beg_index: end_index]
                attributes = attributes[attributes.index('[')+1: attributes.index(']')]
                attributes = attributes.replace('"', "")
                attributes_list = attributes.split(", ")
                self.logger.info("Attributes: " + str(attributes_list))
                df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Attributes")] = attributes_list
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("No attributes found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods


    def update_neighborhood_census_info(self, current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
        """ Update the neighborhood with the census info.  Some neighborhoods have no census info and we will throw an exception.
        """
        try:
            beg_index = neighborhood_info.index('census: {')
            end_index = neighborhood_info.index('city: ')
            if ((beg_index > 0) and (end_index > 0)):
                census = neighborhood_info[beg_index: end_index]
                census = census[census.index('{'): census.index('}')+1]
                census = json.loads(census)
                self.logger.info("Census: %s\n" % str(census))
                df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Percentage of Homeowners")] = int(census['homeowners'])
                df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Number of Residents")] = int(census['population'])
                df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Average Age")] = int(census['age'])
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error("No census info found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

        return df_nextdoor_neighborhoods


    def parse_neighborhoods_ext(self, df_nextdoor_neighborhoods, neighborhood_g):
        """ Main function for parsing each neighborhood.
        """
        current_row_index = 0
        end_row_index = df_nextdoor_neighborhoods.shape[0] - 1

        while True:

            if df_nextdoor_neighborhoods.iloc[current_row_index]["City"] in nds.CITIES:

                neighborhood_info = self.get_current_page_neighborhood_info(current_row_index, df_nextdoor_neighborhoods)

                if len(neighborhood_info) > 0:

                    df_nextdoor_neighborhoods = self.update_current_page_neighborhood_id(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

                    df_nextdoor_neighborhoods = self.add_nearby_neighborhoods(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

                    df_nextdoor_neighborhoods = self.update_nearby_neighborhood_info(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

                    df_nextdoor_neighborhoods, neighborhood_g = self.update_nearby_neighborhood_features(current_row_index, neighborhood_info, df_nextdoor_neighborhoods, neighborhood_g)

                    df_nextdoor_neighborhoods = self.update_neighborhood_interests(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

                    df_nextdoor_neighborhoods = self.update_neighborhood_attributes(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

                    df_nextdoor_neighborhoods = self.update_neighborhood_census_info(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

            current_row_index += 1
            end_row_index = df_nextdoor_neighborhoods.shape[0] - 1

            #Write out the file every 100 entries
            if current_row_index%100 == 0:
                file_handle = open(nds.EDGELIST_FILENAME, 'wb')
                nx.write_edgelist(neighborhood_g, file_handle)
                self.logger.info('Saved file: %s' % nds.EDGELIST_FILENAME)

                df_nextdoor_neighborhoods.to_csv(nds.NEIGHBORHOOD_EXT_FILENAME, index=False)
                self.logger.info('Saved file: %s' % nds.NEIGHBORHOOD_EXT_FILENAME)

            if current_row_index > end_row_index:
                break

        return df_nextdoor_neighborhoods, neighborhood_g

    def scrape_neighborhoods_ext(self):
        """ Parse extended data for each neighborhood
        """
        #Read the neighborhood list that we will update with more fields
        df_nextdoor_neighborhoods = pd.read_csv(nds.NEIGHBORHOOD_FILENAME)

        df_nextdoor_neighborhoods = self.update_neighborhood_df(df_nextdoor_neighborhoods)

        #Create a graph of all the neighborhoods that are near each other
        neighborhood_g = nx.Graph()

        df_nextdoor_neighborhoods, neighborhood_g = self.parse_neighborhoods_ext(df_nextdoor_neighborhoods, neighborhood_g)

        #Save the edgelist
        file_handle = open(nds.EDGELIST_FILENAME, 'wb')
        nx.write_edgelist(neighborhood_g, file_handle)
        self.logger.info('Saved file: %s' % nds.EDGELIST_FILENAME)

        #Write out all the update neighborhoods with all the new fields we added.
        df_nextdoor_neighborhoods.to_csv(nds.NEIGHBORHOOD_EXT_FILENAME, index=False)
        self.logger.info('Saved file: %s' % nds.NEIGHBORHOOD_EXT_FILENAME)

def main():
    nextdoor_neighborhoods_ext = NextdoorNeighborhoodsExt()
    nextdoor_neighborhoods_ext.scrape_neighborhoods_ext()


if __name__ == "__main__":
    main()
