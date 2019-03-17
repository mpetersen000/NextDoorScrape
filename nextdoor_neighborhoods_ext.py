"""Parse extended data for nextdoor neighborhoods.
"""
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

CITIES = ['San Jose', 'Santa Clara', 'Sunnyvale', 'Palo Alto',\
'Mountain View', 'Cupertino', 'Milpitas', 'Los Gatos', 'Gilroy',\
'Morgan Hill', 'Campbell', 'Los Altos', 'Saratoga', 'Stanford',\
'Los Altos Hills', 'San Martin']

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

# Iterate over each neighborhood and update the URL and ID
def find_nearby_neighborhood_ids(neighborhood_info, df_nextdoor_neighborhoods):
    neighborhoodMap = neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
    neighborhoodMap = neighborhoodMap.lstrip()
    neighborhoodMap = neighborhoodMap.rstrip()
    begURLIndexes = list(find_all(neighborhoodMap, "\"page_url\": \""))
    endURLIndexes = list(find_all(neighborhoodMap, "\", \"short_name\": "))
    begShortNameIndexes = list(find_all(neighborhoodMap, "\"short_name\": \""))
    endShortNameIndexes = list(find_all(neighborhoodMap, "\", \"stroke_color\": "))
    begIDIndexes = list(find_all(neighborhoodMap, "\"id\": "))
    endIDIndexes = list(find_all(neighborhoodMap, ", \"geometry\": \"{"))
    if ((len(begURLIndexes) == len(endURLIndexes)) and
        (len(begShortNameIndexes) == len(endShortNameIndexes)) and
        (len(begIDIndexes) == len(endIDIndexes))):
        for begShort, endShort, begURL, endURL, begID, endID in zip(begShortNameIndexes, endShortNameIndexes, begURLIndexes, endURLIndexes, begIDIndexes, endIDIndexes):
            print("Index for name:" + str(begShort) + str(endShort))
            print(begURL, endURL)
            print(begID, endID)
            shortname = neighborhoodMap[begShort + len ("\"short_name\": \""): endShort]
            shortname = re.sub('\s+', ' ', shortname).strip()
            print(shortname)
            entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == shortname]
            if (len(entry) == 1):
                pp.pprint(entry)
                index = int(df_nextdoor_neighborhoods.index[df_nextdoor_neighborhoods['Neighborhood'] == shortname][0])
                print(df_nextdoor_neighborhoods.iloc[index])
                if (len(df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")]) == 0):
                    page_url = neighborhoodMap[begURL + len ("\"page_url\": \""): endURL]
                    print("Updating Page URL: " + page_url)
                    if (len(page_url) > 0):
                        df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Link")] = page_url
                if (df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] == 0):
                    neighborhood_id = neighborhoodMap[begID + len ("\"id\": "): endID]
                    print("ID: " + neighborhood_id)
                    df_nextdoor_neighborhoods.iat[index, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(neighborhood_id)
                print(df_nextdoor_neighborhoods.iloc[index])
                print("findAllTheNeighborhoodIDs loop")
    print("findAllTheNeighborhoodIDs")

def find_nearby_neighborhoods(neighborhood_info, df_nextdoor_neighborhoods):
    """Add neighborhoods that we found as part of scraping. Some neighborhoods are only listed in the nearby neighborhood sections and not on the main page of neighborhoods for a city.
    """
    nearbyNeighborhoods = neighborhood_info[neighborhood_info.index('nearbyNeighborhoods: '): neighborhood_info.index('neighborhoodGeometriesJSON:')]
    nearbyNeighborhoods = nearbyNeighborhoods.lstrip()
    nearbyNeighborhoods = nearbyNeighborhoods.rstrip()
    begIndex = len('nearbyNeighborhoods: ')
    nearbyNeighborhoods = nearbyNeighborhoods[begIndex: -1]
    nearby_neighborhood_json = json.loads(nearbyNeighborhoods)
    for nearby_neighborhood in nearby_neighborhood_json:
        if (nearby_neighborhood['city'] in CITIES):
            nearby_neighborhood['shortName'] = re.sub('\s+', ' ', nearby_neighborhood['shortName']).strip()
            existing_entry = df_nextdoor_neighborhoods.loc[df_nextdoor_neighborhoods['Neighborhood'] == nearby_neighborhood['shortName']]
            if (len(existing_entry) == 0):
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

def find_current_page_neighborhood_id(currentRowIndex, neighborhood_info,\
df_nextdoor_neighborhoods):
    """ Find the ID of the neighborhood the page is for
    """
    neighborhoodMap = neighborhood_info[neighborhood_info.index('neighborhoodMapOptions:{'): -1]
    neighborhoodMap = neighborhoodMap.lstrip()
    neighborhoodMap = neighborhoodMap.rstrip()
    begIDIndex = list(find_all(neighborhoodMap, "hoodId: "))
    assert(len(begIDIndex) == 1)
    endIDIndex = begIDIndex[0] + len("hoodId: ") + 10
    hoodId = neighborhoodMap[begIDIndex[0] + len("hoodId: "): endIDIndex]
    hoodId = hoodId.lstrip()
    hoodId = hoodId.rstrip()
    print(hoodId)
    hoodId = re.findall(r'^([\s\d]+)$', hoodId)
    print(hoodId)
    assert(len(hoodId) == 1)
    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Nextdoor ID")] = int(hoodId[0])

    return df_nextdoor_neighborhoods

def get_neighborhood_info(currentRowIndex, df_nextdoor_neighborhoods):
    """ Find the variable on the page with all the neighborhood details
    """
    neighborhood_info = ""
    if len(df_nextdoor_neighborhoods.iloc[currentRowIndex]["Link"]) > 0:
        neighborhood_lookup =  df_nextdoor_neighborhoods.iloc[currentRowIndex]["Link"]
        html_soup, response = make_request(neighborhood_lookup)

        scripts = html_soup.findAll('script')
        neighborhood_info = scripts[5].string.strip()
        neighborhood_info = neighborhood_info[neighborhood_info.index('['):-1]

    return neighborhood_info

def iterateOneNeighborhood(currentRowIndex):

            findAllTheNeighborhoodIDs(neighborhood_info)
            pp.pprint(df_nextdoor_neighborhoods.iloc[currentRowIndex])

            findAllTheNeighborhoodFeatures(neighborhood_info)
            pp.pprint(df_nextdoor_neighborhoods.iloc[currentRowIndex])

            # Update with the interest info
            try:
                begInterestIndex = neighborhood_info.index('interests: [')
                endInterestIndex = neighborhood_info.index('iosUrl: "https://')
                if ((begInterestIndex > 0) and (endInterestIndex > 0)):
                    interests = neighborhood_info[begInterestIndex: endInterestIndex]
                    interests = interests[interests.index('[')+1: interests.index(']')]
                    interests = interests.replace('"', "")
                    interest_list = interests.split(", ")
                    print(interest_list)
                    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Interests")] = interest_list
                    if ('Gardening & Landscape' in interest_list):
                        df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Gardening Interest")] = interest_list.index('Gardening & Landscape') + 1
            except:
                print("No interests found for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

            # Update with the attribute info
            try:
                begAttrIndex = neighborhood_info.index('attributes: [')
                endAttrIndex = neighborhood_info.index('census:')
                if ((begAttrIndex > 0) and (endAttrIndex > 0)):
                    attributes = neighborhood_info[begAttrIndex: endAttrIndex]
                    attributes = attributes[attributes.index('[')+1: attributes.index(']')]
                    attributes = attributes.replace('"', "")
                    attributes_list = attributes.split(", ")
                    print(attributes_list)
                    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Attributes")] = attributes_list
            except:
                print("No attributes found for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

            # Update with the census info
            try:
                begCensusIndex = neighborhood_info.index('census: {')
                endCensusIndex = neighborhood_info.index('city: ')
                if ((begCensusIndex > 0) and (endCensusIndex > 0)):
                    census = neighborhood_info[begCensusIndex: endCensusIndex]
                    census = census[census.index('{'): census.index('}')+1]
                    census = json.loads(census)
                    print("Census: %s\n" % str(census))
                    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Percentage of Homeowners")] = int(census['homeowners'])
                    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Number of Residents")] = int(census['population'])
                    df_nextdoor_neighborhoods.iat[currentRowIndex, df_nextdoor_neighborhoods.columns.get_loc("Average Age")] = int(census['age'])
            except:
                print("No census found for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

        except:
            print("Error parsing")
    return (neighborhood_info)

def parse_neighborhoods_ext(df_nextdoor_neighborhoods):
    currentRowIndex = 0
    neighborhood_info = get_neighborhood_info(currentRowIndex, df_nextdoor_neighborhoods)

    try:
        df_nextdoor_neighborhoods = find_current_page_neighborhood_id(currentRowIndex, neighborhood_info, df_nextdoor_neighborhoods)
        pp.pprint("Neighborhood after find_current_page_neighborhood_id: " +  str(df_nextdoor_neighborhoods.iloc[currentRowIndex])
    except:
        print("Error find_current_page_neighborhood_id for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

    try:
        df_nextdoor_neighborhoods = find_nearby_neighborhoods(currentRowIndex, neighborhood_info, df_nextdoor_neighborhoods)
        pp.pprint("Neighborhood after find_current_page_neighborhood_id: " +  str(df_nextdoor_neighborhoods.iloc[currentRowIndex])
    except:
        print("Error find_nearby_neighborhoods for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

    try:
        df_nextdoor_neighborhoods = find_nearby_neighborhoods(currentRowIndex, neighborhood_info, df_nextdoor_neighborhoods)
        pp.pprint("Neighborhood after find_current_page_neighborhood_id: " +  str(df_nextdoor_neighborhoods.iloc[currentRowIndex])
    except:
        print("Error find_nearby_neighborhoods for: ", df_nextdoor_neighborhoods.iloc[currentRowIndex]["Neighborhood"])

    return df_nextdoor_neighborhoods

def scrape_neighborhoods_ext():
    df_nextdoor_neighborhoods = pd.read_csv(NEIGHBORHOOD_FILENAME)

    df_nextdoor_neighborhoods =
    update_neighborhood_df(df_nextdoor_neighborhoods)

    df_next_door_neighborhoods =
    parse_neighborhoods_ext(df_nextdoor_neighborhoods)

    df_nextdoor_neighborhoods.to_csv(NEIGHBORHOOD_EXT_FILENAME, index=False)
    print('Saved file: %s' % NEIGHBORHOOD_EXT_FILENAME)


if __name__ == "__main__":
    print(scrape_neighborhoods_ext())
