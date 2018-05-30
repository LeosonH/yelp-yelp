#-------------------------------------------------------------------------------
# Name:        build_db
# Purpose:     Construct Sql database from csv files in order to join them.
#              Ultimately, we'll construct a new "master.csv" making our
#              computations much more efficient.
#
# Author:      alex
#
# Created:     27/05/2018
#-------------------------------------------------------------------------------

import sqlite3
import pandas as pd

def run():

    db = sqlite3.connect("yelp_db.db")

    business = pd.read_csv("yelp_business.csv")
    business_hours = pd.read_csv("yelp_business_hours.csv")
    business_attributes = pd.read_csv("yelp_business_attributes.csv")

    business.to_sql('business', con = db, flavor = 'sqlite')
    business_hours.to_sql('business_hours', con = db, flavor = 'sqlite')
    business_attributes.to_sql('business_attributes', con = db, \
    flavor = 'sqlite')



    # The review csv is way too large to fit in menory, so we used a generator
    # and a little creativity here
    review_generator = pd.read_csv("yelp_business.csv", iterator = True, \
    chunksize = 5000)

    while True:
        try:
            review_chunk = review_generator.get_chunk()
            review_chunk.to_sql('review', con = db, flavor = 'sqlite', \
            if_exists = "append")
        except:
            break


if __name__ == "__main__":
    run()