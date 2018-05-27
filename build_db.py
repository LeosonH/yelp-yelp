#-------------------------------------------------------------------------------
# Name:        module2
# Purpose:
#
# Author:      alex
#
# Created:     27/05/2018
# Copyright:   (c) alex 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sqlite3
import pandas as pd

db = sqlite3.connect("yelp_db.db")

business = pd.read_csv("yelp_business.csv")
business_hours = pd.read_csv("yelp_business_hours.csv")
business_attributes = pd.read_csv("yelp_business_attributes.csv")

business.to_sql('business', con = db, flavor = 'sqlite')
business_hours.to_sql('business_hours', con = db, flavor = 'sqlite')
business_attributes.to_sql('business_attributes', con = db, flavor = 'sqlite')

review_generator = pd.read_csv("yelp_business.csv", iterator = True, chunksize = 5000)

while True:
    try:
        review = test.get_chunk()
        pd.to_sql('review', con = db, flavor = 'sqlite', index = True, if_exists = "append")
    except:
        break
