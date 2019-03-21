"""Parse extended data for nextdoor neighborhoods.
"""
import re
import json
import pandas as pd
import requests
import pprint as pp
from bs4 import BeautifulSoup

#CITIES = ['San Jose', 'Santa Clara', 'Sunnyvale', 'Palo Alto',\
#'Mountain View', 'Cupertino', 'Milpitas', 'Los Gatos', 'Gilroy',\
#'Morgan Hill', 'Campbell', 'Los Altos', 'Saratoga', 'Stanford',\
#Los Altos Hills', 'San Martin']

CITIES = ['San Martin']

NEIGHBORHOOD_EXT_FILENAME = 'nextdoor_california_neighborhoods_ext.csv'
NEIGHBORHOOD_FILENAME = 'nextdoor_california_neighborhoods.csv'
STATE_FILENAME = 'nextdoor_california_cities.csv'


def find_all(a_str, name):
    """Find all the values in a dictionary string after name
    """
    start = 0
    while True:
        start = a_str.find(name, start)
        if start == -1:
            return
        yield start
        start += len(name)


def make_request(url):
    """Make a request to retrieve html from url
    """
    response = requests.get(url)
    response_text = response.text.encode('UTF-8')
    html_soup = BeautifulSoup(response_text, 'html.parser')
    return html_soup


def update_neighborhood_df(df_nextdoor_neighborhoods):
    """Add columns for new features
    """
    df_nextdoor_neighborhoods["Nextdoor ID"] = 0
    df_nextdoor_neighborhoods["Geometry"] = ""
    df_nextdoor_neighborhoods["Interests"] = ""
    df_nextdoor_neighborhoods["Percentage of Homeowners"] = 0
    df_nextdoor_neighborhoods["Number of Residents"] = 0
    df_nextdoor_neighborhoods["Average Age"] = 0
    df_nextdoor_neighborhoods['Attributes'] = ""

    df_nextdoor_neighborhoods["Geometry"].fillna("", inplace=True)
    df_nextdoor_neighborhoods["Interests"].fillna("", inplace=True)
    df_nextdoor_neighborhoods["Attributes"].fillna("", inplace=True)

    return df_nextdoor_neighborhoods

def update_nearby_neighborhood_info(neighborhood_info, df_nextdoor_neighborhoods):
    """Iterate over each neighborhood and update the URL and ID from the \
    information in the neighborhood map variable.\
    """
    neighborhood_map = neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
    neighborhood_map = neighborhood_map.strip()
    beg_url_indexes = list(find_all(neighborhood_map, "\"page_url\": \""))
    end_url_indexes = list(find_all(neighborhood_map, "\", \"short_name\": "))
    beg_shortname_indexes = list(find_all(neighborhood_map, "\"short_name\": \""))
    end_shortname_indexes = list(find_all(neighborhood_map, "\", \"stroke_color\": "))
    beg_id_indexes = list(find_all(neighborhood_map, "\"id\": "))
    end_id_indexes = list(find_all(neighborhood_map, ", \"geometry\": \"{"))
    if ((len(beg_url_indexes) == len(end_url_indexes)) and\
    (len(beg_shortname_indexes) == len(end_shortname_indexes)) and\
    (len(beg_id_indexes) == len(end_id_indexes))):
        for beg_short, end_short, beg_url, end_url, beg_id, end_id in\
        zip(beg_shortname_indexes, end_shortname_indexes,\
        beg_url_indexes, end_url_indexes,\
        beg_id_indexes, end_id_indexes):
            print(beg_short, end_short)
            print(beg_url, end_url)
            print(beg_id, end_id)
            shortname = neighborhood_map[beg_short + len("\"short_name\": \"") : end_short]
            shortname = re.sub(r'\s+', ' ', shortname).strip()
            print(shortname)
            entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == shortname]
            if len(entry) == 1:
                index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Neighborhood'] == shortname][0])
                print(df_nextdoor_neighborhoods.iloc[index])
                if len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")]) == 0:
                    page_url = neighborhood_map[beg_url + len("\"page_url\" : \"") : end_url]
                    print("Updating Page URL: " + page_url)
                    if len(page_url) > 0:
                        df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")] = page_url
                    if df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0:
                        neighborhood_id = neighborhood_map[beg_id + len("\"id\": ") : end_id]
                        print("Updating Nextdoor ID: " + neighborhood_id)
                        df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(neighborhood_id)
                    print(df_nextdoor_neighborhoods.iloc[index])

    return df_nextdoor_neighborhoods


def add_nearby_neighborhoods(neighborhood_info, df_nextdoor_neighborhoods):
    """Add neighborhoods that we found as part of scraping. Some neighborhoods\
    are only listed in the nearby neighborhood sections and not on the main\
    page of neighborhoods for a city.
    """
    nearby_neighborhoods =\
    neighborhood_info[neighborhood_info.index('nearbyNeighborhoods: '): neighborhood_info.index('neighborhoodGeometriesJSON:')]
    nearby_neighborhoods = nearby_neighborhoods.lstrip()
    nearby_neighborhoods = nearby_neighborhoods.rstrip()
    beg_index = len('nearbyNeighborhoods: ')
    nearby_neighborhoods = nearby_neighborhoods[beg_index: -1]
    nearby_neighborhood_json = json.loads(nearby_neighborhoods)
    for nearby_neighborhood in nearby_neighborhood_json:
        if nearby_neighborhood['city'] in CITIES:
            nearby_neighborhood['shortName'] = re.sub(r'\s+', ' ', nearby_neighborhood['shortName']).strip()
            existing_entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == nearby_neighborhood['shortName']]
            if len(existing_entry) == 0:
                print("Adding Neighborhood: ", nearby_neighborhood)
                df_nextdoor_neighborhoods = df_nextdoor_neighborhoods.append(
                    {"State": nearby_neighborhood['state'],
                     #TODO
                     "County": "Santa Clara",
                     "City": nearby_neighborhood['city'],
                     "Neighborhood": nearby_neighborhood['shortName'],
                     "Link": "",
                     "Nextdoor ID": 0,
                     "Geometry": "",
                     "Interests": "",
                     "Percentage of Homeowners": 0,
                     "Number of Residents": 0,
                     "Average Age": 0,
                     "Attributes": ""
                    }, ignore_index=True)

    return df_nextdoor_neighborhoods

def update_current_page_neighborhood_id(current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
    """ Find the ID of the neighborhood the page is for
    """
    neighborhood_map = neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
    neighborhood_map = neighborhood_map.strip()
    beg_index = list(find_all(neighborhood_map, "hoodId: "))
    assert len(beg_index) == 1
    end_index = beg_index[0] + len("hoodId: ") + 10
    hood_id = neighborhood_map[beg_index[0] + len("hoodId: "): end_index]
    hood_id = hood_id.strip()
    hood_id = re.findall(r'^([\s\d]+)$', hood_id)
    assert len(hood_id) == 1
    df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(hood_id[0])

    return df_nextdoor_neighborhoods

def update_nearby_neighborhood_ids(current_row_index, neighborhood_info,\
df_nextdoor_neighborhoods):
    """ Find the ids of the nearby neighborhoods.
    """
    # Get the neighborhood map data that contains all the details for each
    # nearby neighborhood
    neighborhood_map =  neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
    neighborhood_map = neighborhood_map.rstrip()
    # Get the indexes of the data we care about namely the link, name, id, and # geometry
    beg_url_indexes = list(find_all(neighborhood_map, "\"page_url\": \""))
    end_url_indexes = list(find_all(neighborhood_map, "\", \"short_name\": "))
    beg_shortname_indexes = list(find_all(neighborhood_map, "\"short_name\": \""))
    end_shortname_indexes = list(find_all(neighborhood_map, "\", \"stroke_color\": "))
    beg_id_indexes = list(find_all(neighborhood_map, "\"id\": "))
    end_id_indexes = list(find_all(neighborhood_map, ", \"geometry\": \"{"))
    if ((len(beg_url_indexes) == len(end_url_indexes)) and (len(beg_shortname_indexes) == len(end_shortname_indexes)) and
    (len(beg_id_indexes) == len(end_id_indexes))):
        for beg_short, end_short, beg_url, end_url, beg_id, end_id in zip(beg_shortname_indexes, end_shortname_indexes, beg_url_indexes, end_url_indexes, beg_id_indexes, end_id_indexes):
            shortname = neighborhood_map[beg_short + len ("\"short_name\": \""): end_short]
            shortname = re.sub(r'\s+', ' ', shortname).strip()
            print("Updating neighborhood ID for: " + shortname)
            # Find the existing entry to update.
            #TODO: We should also check the city and county since neighborhood names are not unique.
            entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == shortname]
            if len(entry) == 1:
                index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Neighborhood'] == shortname][0])
                if len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")]) == 0:
                    page_url = neighborhood_map[beg_url + len("\"page_url\": \""): end_url]
                    print("Updating Page URL: " + page_url)
                    if len(page_url) > 0:
                        df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")] = page_url
                if (df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0):
                    neighborhood_id = neighborhood_map[beg_id + len("\"id\": "): end_id]
                    print("Updating ID: " + neighborhood_id)
                    df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(neighborhood_id)

    return df_nextdoor_neighborhoods


def update_nearby_neighborhood_features(current_row_index, neighborhood_info,\
df_nextdoor_neighborhoods):
    """ Update the geometry of each neighborhood.
    """
    neighborhood_geometries = neighborhood_info[neighborhood_info.index('neighborhoodGeometriesJSON: '): neighborhood_info.index('neighborhoodMapOptions:{')]
    neighborhood_geometries = neighborhood_geometries.strip()
    beg_index = len('neighborhood_geometriesJSON: ') - 1
    neighborhood_geometries = neighborhood_geometries[beg_index: -1]
    print(neighborhood_geometries)
    neighborhood_json = json.loads(neighborhood_geometries)
    for feature in neighborhood_json["features"]:
        props = feature["properties"]
        props["hood_name"] = re.sub(r'\s+', ' ', props['hood_name']).strip()
        print("Updating features for: %s, ID: %s \n" % (props["hood_name"], props["hood_id"]))
        entry = df_nextdoor_neighborhoods.loc[(df_nextdoor_neighborhoods['Neighborhood'] == props["hood_name"]) &                                              (df_nextdoor_neighborhoods['Nextdoor ID'] == props["hood_id"])]
        if len(entry) == 1:
            index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Nextdoor ID'] == props["hood_id"]][0])
            #Save the feature for that neighborhood without the featurecollection
            if len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Geometry")]) == 0:
                feature['properties']['State'] = df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("State")]
                feature['properties']['City'] = df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("City")]
                feature['properties']['County'] = "Santa Clara"
                feature['properties']['Neighborhood'] = props["hood_name"]
                feature['properties']['Nextdoor ID'] = props["hood_id"]
                del feature['properties']['hood_name']
                del feature['properties']['hood_id']
                del feature['properties']['fill']
                del feature['properties']['stroke']
                del feature['properties']['fill-opacity']
                df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Geometry")] = json.dumps(feature)

            if (df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0):
                df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = props["hood_id"]

            print(df_nextdoor_neighborhoods.iloc[index])

    return df_nextdoor_neighborhoods


def get_current_page_neighborhood_info(current_row_index, df_nextdoor_neighborhoods):
    """ Find the variable on the page with all the neighborhood details
    """
    neighborhood_info = ""
    if len(df_nextdoor_neighborhoods.iloc[current_row_index]["Link"]) > 0:
        neighborhood_lookup = df_nextdoor_neighborhoods.iloc[current_row_index]["Link"]
        html_soup = make_request(neighborhood_lookup)

        scripts = html_soup.findAll('script')
        neighborhood_info = scripts[5].string.strip()
        neighborhood_info = neighborhood_info[neighborhood_info.index('['):-1]

    return neighborhood_info


def update_neighborhood_interests(current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
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
            print(interest_list)
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Interests")] = interest_list
    except Exception as e:
        print(e)
        print("No interests found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

    return df_nextdoor_neighborhoods


def update_neighborhood_attributes(current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
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
            print(attributes_list)
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Attributes")] = attributes_list
    except Exception as e:
        print(e)
        print("No attributes found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

    return df_nextdoor_neighborhoods


def update_neighborhood_census_info(current_row_index, neighborhood_info, df_nextdoor_neighborhoods):
    """ Update the neighborhood with the census info.
    """
    try:
        beg_index = neighborhood_info.index('census: {')
        end_index = neighborhood_info.index('city: ')
        if ((beg_index > 0) and (end_index > 0)):
            census = neighborhood_info[end_index: end_index]
            census = census[census.index('{'): census.index('}')+1]
            census = json.loads(census)
            print("Census: %s\n" % str(census))
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Percentage of Homeowners")] = int(census['homeowners'])
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Number of Residents")] = int(census['population'])
            df_nextdoor_neighborhoods.iat[current_row_index, df_nextdoor_neighborhoods.columns.get_loc("Average Age")] = int(census['age'])
    except Exception as e:
        print(e)
        print("No census info found for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

    return df_nextdoor_neighborhoods


def parse_neighborhoods_ext(df_nextdoor_neighborhoods):
    current_row_index = 0
    end_row_index = df_nextdoor_neighborhoods.shape[0] - 1

    while True:

        if df_nextdoor_neighborhoods.iloc[current_row_index]["City"] in CITIES:

            neighborhood_info = get_current_page_neighborhood_info(current_row_index, df_nextdoor_neighborhoods)

            try:
                df_nextdoor_neighborhoods = update_current_page_neighborhood_id(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)
                print("Neighborhood after update_current_page_neighborhood_id: " + str(df_nextdoor_neighborhoods.iloc[current_row_index]))
            except Exception as e:
                print(e)
                print("Error update_current_page_neighborhood_id for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

            try:
                df_nextdoor_neighborhoods = add_nearby_neighborhoods(neighborhood_info, df_nextdoor_neighborhoods)
                print("Neighborhood after add_nearby_neighborhoods: " +  str(df_nextdoor_neighborhoods.iloc[current_row_index]))
            except Exception as e:
                print(e)
                print("Error add_nearby_neighborhoods for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

            try:
                df_nextdoor_neighborhoods = update_nearby_neighborhood_info(neighborhood_info, df_nextdoor_neighborhoods)
                print("Neighborhood after update_nearby_neighborhood_info: " +  str(df_nextdoor_neighborhoods.iloc[current_row_index]))
            except Exception as e:
                print(e)
                print("Error update_nearby_neighborhood_info for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

            try:
                df_nextdoor_neighborhoods = update_nearby_neighborhood_ids(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)
                print("Neighborhood after update_nearby_neighborhood_ids: " +  str(df_nextdoor_neighborhoods.iloc[current_row_index]))
            except Exception as e:
                print(e)
                print("Error update_nearby_neighborhood_info for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

            try:
                df_nextdoor_neighborhoods = update_nearby_neighborhood_features(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)
                print("Neighborhood after update_nearby_neighborhood_features: " +  str(df_nextdoor_neighborhoods.iloc[current_row_index]))
            except Exception as e:
                print(e)
                print("Error update_nearby_neighborhood_features for: ", df_nextdoor_neighborhoods.iloc[current_row_index]["Neighborhood"])

            df_nextdoor_neighborhoods = update_neighborhood_interests(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

            df_nextdoor_neighborhoods = update_neighborhood_attributes(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

            df_nextdoor_neighborhoods = update_neighborhood_census_info(current_row_index, neighborhood_info, df_nextdoor_neighborhoods)

        current_row_index += 1
        end_row_index = df_nextdoor_neighborhoods.shape[0] - 1

        if current_row_index > end_row_index:
            break

    return df_nextdoor_neighborhoods

def scrape_neighborhoods_ext():
    df_nextdoor_neighborhoods = pd.read_csv(NEIGHBORHOOD_FILENAME)

    df_nextdoor_neighborhoods = update_neighborhood_df(df_nextdoor_neighborhoods)

    df_nextdoor_neighborhoods = parse_neighborhoods_ext(df_nextdoor_neighborhoods)

    df_nextdoor_neighborhoods.to_csv(NEIGHBORHOOD_EXT_FILENAME, index=False)
    print('Saved file: %s' % NEIGHBORHOOD_EXT_FILENAME)


if __name__ == "__main__":
    print(scrape_neighborhoods_ext())
