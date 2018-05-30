# CS123 Spring '18
# Calculate similarity scores for business pairs
# Lily Li

# to save output to csv: python3 calculate_success_scores.py --jobconf mapreduce.job.reduces=1 <data/yelp_business_sample.csv> output.csv
# python3 calculate_success_scores.py --hours data/yelp_hours_samp.csv --attributes data/yelp_business_attributes_samp.csv data/yelp_business_sample.csv > data.txt
# helpful link: http://jmedium.com/mapreduce-additionalfile/


from mrjob.job import MRJob
from mrjob import protocol
import csv
from math import radians, cos, sin, asin, sqrt
import re


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
    Calculate the star rating similarity between two businesses
        as a percentage. 100% is having the same average star rating
    """
    MAX_STARS = 5
    return (MAX_STARS - abs(rating1 - rating2)) / MAX_STARS


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


class MRSuccessScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    # use an output protocol to format output nicely
    OUTPUT_PROTOCOL = protocol.TextProtocol

    def configure_options(self):
        super(MRSuccessScores, self).configure_options()
        # add extra files we need to join with our main file
        self.add_file_option('--businesses_to_pair', help='path to yelp_business_sample.csv')
        # add this file again to create pairs
        self.add_file_option('--hours', help='path to yelp_hours_samp.csv')
        self.add_file_option('--attributes', help='path to yelp_business_attributes_samp.csv') 


    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and calculates the
        success score for the business.

        Inputs:
            self: an instance of the MRSuccessScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key value pair of the person's name and their status
        '''
        business = next(csv.reader([line]))
        # first file to be processed must be "main" file (i.e. not a file in add_file_option())
        # each mapper has one main business_id that it matches pairs with
        biz_id = business[0]
        stars = business[9]
        review_count = business[10]
        is_open = business[11]
        categories = business[12]

        biz_hours = []
        biz_attributes = []

        scores = []

        with open('yelp_business_sample.csv') as f:
        # iterate through second copy of businesses file to create pairs
            for line in f:
                second_biz = line.split(',')
                second_biz_id = fields[0]
                if second_biz_id != bus_id:
                    second_biz_hours = 



        with open('yelp_hours_samp.csv') as f:
            for line in f:
                print(line)
                fields = line.split(',')
                attr_bus_id = fields[0]
                if attr_bus_id != business_id:
                    score = fields[1]
                    break

        with open('yelp_business_attributes_samp.csv') as f:
            for line in f:
                fields = line.split(',')
                attr_bus_id = fields[0]
                if attr_bus_id == business_id:
                    score2 = fields[1]
                    break


        yield business_id, 0


if __name__ == '__main__':
    MRSuccessScores.run()
