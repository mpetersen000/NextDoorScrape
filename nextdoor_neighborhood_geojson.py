"""Export geographic information for nextdoor neighborhoods.
"""
import json
import pandas as pd
import nextdoor_scraping as nds
import geojson
from geojson import Point, Feature, FeatureCollection, dump


def convert_to_geojson(df_nextdoor_neighborhoods, logger):
    """Convert geometry to geojson
    """
    features = []

    #Iterate through each neighborhood
    for index, row in df_nextdoor_neighborhoods.iterrows():
        geometry = row["Geometry"]
        try:
            #Convert the string to valid json and then add the feature
            geometry = json.loads(geometry)
            features.append(geometry)
        except Exception as ex:
            logger.error(ex)
            logger.error("No geometry found for: " + str(row["Neighborhood"]))

    #Create a feature collection with the features
    feature_collection = FeatureCollection(features)

    #Save the geojson to a file
    with open(nds.GEOJSON_FILENAME, 'w') as f:
        dump(feature_collection, f)

    logger.info('Saved file: %s' % nds.GEOJSON_FILENAME)


def convert_neighborhood_to_geojson():
    """ Read in the nextdoor neighborhood file and convert to geojson
    """
    #Create the logger
    logger = nds.create_logger()
    df_nextdoor_neighborhoods = pd.read_csv(nds.NEIGHBORHOOD_EXT_FILENAME)

    convert_to_geojson(df_nextdoor_neighborhoods, logger)


if __name__ == "__main__":
    print(convert_neighborhood_to_geojson())
