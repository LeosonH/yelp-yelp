import pandas as pd


def go(filename):
    sample = pd.read_csv(filename, usecols = ['business_id', 'latitude',
        'longitude', 'stars', 'review_count', 'categories'])[:100]

    rows_list = []

    for row in sample.itertuples():
        success_score = row[4] * row[5]
        rows_list.append({'business_id': row[1], 'success': success_score})

    return pd.DataFrame(rows_list)


if __name__ == "__main__":
    output_df = go('data/yelp_business.csv')
    output_df.to_csv('data/yelp_business_success_scores_sample.csv', index = False)
