### sample code to calculate similarity scores between the first 100 rows of Yelp businesses

import pandas as pd
import json
from math import radians, cos, sin, asin, sqrt
import re


# with open("data/categories.json") as f:
# # source: https://www.yelp.com/developers/documentation/v3/all_category_list
# # https://www.yelp.com/developers/documentation/v3/all_category_list/categories.json
# COULD DO A WEIGHTING SCALE: check level on the treemap
#     categories_list = json.load(f)
#     # categories is a list of dicts

#     # need to create two dicts with different keys b/c
#     # the cats in the yelp dataset are titles, but
#     # cat parents are aliases
#     categories_keys_titles = {}
#     for cat in categories_list:
#        name = cat['title']
#        categories_keys_titles[name] = cat

#     categories_keys_aliases = {}
#     for cat in categories_list:
#        name = cat['alias']
#        categories_keys_aliases[name] = cat


def calculate_haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the haversine distance in miles between two businesses
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 3956 # radius of earth in miles
    return c * r


def calculate_stars_sim(rating1, rating2):
    """
    Calculate the star rating similarity in miles between two businesses
        as a percentage. 100% is having the same average star rating
    """
    MAX_STARS = 5
    return ((5 - abs(rating1 - rating2)) / 5) * 100


# def calculate_category_sim(categories1, categories2):

#     for cat_list in (categories1, categories2):
#         for cat in cat_list:

def find_hours(business_id, samp_hours):
    for i in samp_hours:
        if i[0] == business_id:
            hours = []
            for j in range(1,8):
                start, end = i[j].split("-")
                start_hrs_mins = re.match("([0-9]*):?([0-9]*)", start)
                start_frac = int(start_hrs_mins.group(2)) / 60
                end_hrs_mins = re.match("([0-9]*):?([0-9]*)", end)
                end_frac = int(end_hrs_minutes.group(2)) / 60
                fixed_start = int(start_hrs_mins.group(1)) + str(start_frac)
                fixed_end = int(end_hrs_mins.group(1)) + str(end_frac)
                hours.append((fixed_start, fixed_end))
            return hours

def hours_overlap(id1, id2):
    h1 = find_hours(id1)
    h2 = find_hours(id2)
    overlap = 0
    h1_hours = 0
    for day in range(7):
        h1_hours += h1[day][1] - h1[day][0]
        both_open = min(h1[day][1] - h2[day][0], h2[day][1] - h1[day][0])
        if both_open > 0:
            overlap += both_open
    return overlap / h1_hours

def go(path):
    sample_business = pd.read_csv(path + "yelp_business_samp.csv", usecols = ['business_id', 'latitude',
        'longitude', 'stars', 'review_count', 'categories'])

    sample_hours = pd.read_csv(path + "yelp_hours_samp.csv")

    sample_attributes = pd.read_csv(path + "yelp_business_attributes_samp.csv")

    rows_list = []

    for row1 in sample.itertuples():
        for row2 in sample.itertuples():
            if row1[0] != row2[0]:
                hours_overlap = hours_overlap(row1[0], row2[0])
                distance = calculate_haversine_distance(row1[3], row1[2], row2[3], row2[2])
                stars_sim = calculate_stars_sim(row1[4], row2[4])
                review_count_sim = abs(row1[5] - row2[5]) / min(row1[5], row2[5])
                categories1 = row1[6].split(';')
                categories2 = row2[6].split(';')
                category_sim = (len(set(categories1).intersection(set(categories2))) \
                    / (len(categories1) + len(categories2))) + 1
                # added 1 to deal with scores of 0 turning sim score into 0
                sim_score = (1 / distance) * stars_sim * review_count_sim * category_sim * hours_overlap
                rows_list.append({'business_id1': row1[1],
                    'business_id2': row2[1], 'similarity': sim_score})

    return pd.DataFrame(rows_list)


if __name__ == "__main__":
    output_df = go("C:/Users/alex/Desktop/yelp_data/yelp-dataset/")
    output_df.to_csv('data/yelp_business_similarity_scores_sample.csv', index = False)
