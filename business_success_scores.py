import pandas as pd
import sys

def find_time_open(id, sample_reviews):
    for l in sample_reviews:
        earliest = None
        latest = None
        if l[2] == id:
            date = time.strptime(l[4], "%Y-%m-%d")
            if date < earliest:
                earliest = date
            if date > latest:
                latest = date

def go(path):

    sample_business = pd.read_csv(path + "yelp_business_samp.csv", usecols = ['business_id', 'latitude',
        'longitude', 'stars', 'review_count', 'categories'])

    sample_reviews = pd.read_csv(path + "yelp_review_samp.csv")

    rows_list = []

    for row in sample_business.itertuples():

        success_score = row[3] * row[4]
        rows_list.append({'business_id': row[1], 'success': success_score})

    return pd.DataFrame(rows_list)


if __name__ == "__main__":
    output_df = go("C:/Users/alex/Desktop/yelp_data/yelp-dataset/")
    output_df.to_csv('data/yelp_business_success_scores_sample.csv', index = False)

# C:/Users/alex/Desktop/yelp_data/yelp-dataset/yelp_business_hours.csv
# C:/Users/alex/Desktop/yelp_data/yelp-dataset/yelp_business_attributes.csv
# C:/Users/alex/Desktop/yelp_data/yelp-dataset/yelp_review.csv