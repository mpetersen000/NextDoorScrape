"""Export geographic information for nextdoor neighborhoods.
"""
import json
import pandas as pd
import nextdoor_scraping
import geojson
from geojson import Point, Feature, FeatureCollection, dump


def convert_to_geojson(df_nextdoor_neighborhoods):
    """Convert geometry to geojson
    """
    features = []
    for index, row in df_nextdoor_neighborhoods.iterrows():
        geometry = row["Geometry"]
        try:
            geometry = json.loads(geometry)
            features.append(geometry)
        except Exception as e:
            print(e)
            print("No geometry found for: " + str(row["Neighborhood"]))

    feature_collection = FeatureCollection(features)

    with open(nextdoor_scraping.GEOJSON_FILENAME, 'w') as f:
        dump(feature_collection, f)

    print('Saved file: %s' % nextdoor_scraping.GEOJSON_FILENAME)


def convert_neighborhood_to_geojson():
    """ Read in the nextdoor neighborhood file and convert to geojson
    """
    df_nextdoor_neighborhoods = pd.read_csv(nextdoor_scraping.NEIGHBORHOOD_EXT_FILENAME)

    convert_to_geojson(df_nextdoor_neighborhoods)


if __name__ == "__main__":
    print(convert_neighborhood_to_geojson())
