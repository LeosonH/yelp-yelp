#-------------------------------------------------------------------------------
# Name:        final_mrjob_scores
# Purpose:     Similarity & Success Score mapreduce code
#
# Author(s):   Alex and Lily
#
# Created:     28/05/2018
#-------------------------------------------------------------------------------
# to run: python3 final_mrjob_scores.py --master /home/student/Downloads/master.csv -r 
# dataproc --num-core-instances 125 /home/student/Downloads/master6.csv > mrjob_scores6.csv


from mrjob.job import MRJob
from mrjob import protocol
import csv
from math import radians, cos, sin, asin, sqrt
import re

class MRScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    OUTPUT_PROTOCOL = protocol.TextProtocol


    def configure_options(self):
        super(MRScores, self).configure_options()
        self.add_file_option('--master', help='path to small.csv')


    def calculate_haversine_distance(self, lon1, lat1, lon2, lat2):
        '''
        Calculate the haversine distance in miles between two businesses
        '''
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 3956 # radius of earth in miles
        return c * r


    def format_hours(self, input_hours):
        '''
        Proccesses the business hours and outputs an easier-to-work-with
        tuple ( open time , close time ) with decimals rather than minutes.
        '''
        hours = []
        for i in input_hours:
            if re.match(".+:.+", i):
                start, end = i.split("-")
                start_hrs_mins = re.match("([0-9]*):?([0-9]*)", start)
                start_frac = int(start_hrs_mins.group(2)) / 60
                end_hrs_mins = re.match("([0-9]*):?([0-9]*)", end)
                end_frac = int(end_hrs_mins.group(2)) / 60
                fixed_start = int(start_hrs_mins.group(1)) + start_frac
                fixed_end = int(end_hrs_mins.group(1)) + end_frac
                hours.append((fixed_start, fixed_end))
            else:
                hours.append((0,0))
        return hours


    def hours_overlap(self, hours1, hours2):
        '''
        For a given two businesses, computes the fraction of time that
        businesses A and B are open divided by the total time that A is open.
        '''
        h1 = self.format_hours(hours1)
        h2 = self.format_hours(hours2)

        overlap = 0
        h1_hours = 0
        for day in range(7):
            h1_hours += h1[day][1] - h1[day][0]
            both_open = min(h1[day][1] - h2[day][0], h2[day][1] - h1[day][0])
            if both_open > 0:
                overlap += both_open
        if h1_hours != 0:
            return overlap / h1_hours
        else:
            return 0


    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and pairs it
        with all other business in the file to calculate a similarity score with
        all other local businesses.
        Also calculates the business's success score.

        Inputs:
            self: an instance of the MRScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key-value pair of the business id & the scores

        '''
        bus1 = next(csv.reader([line]))
        business_id1 = bus1[0]
        lat1 = float(bus1[7])
        lon1 = float(bus1[8])
        stars1 = float(bus1[9])
        rev_count1 = int(bus1[10])
        categories1 = bus1[12]
        hours1 = bus1[13:20]
        vader_sentiment = float(bus1[20])

        sim_score_total = 0

        with open('master.csv', encoding = 'utf-8') as f:
            reader = csv.reader(f)
            for bus2 in reader:
                business_id2 = bus2[0]
                lat2 = float(bus2[7])
                lon2 = float(bus2[8])
                stars2 = float(bus2[9])
                rev_count2 = int(bus2[10])
                categories2 = bus2[12]
                hours2 = bus2[13:20]

                distance = self.calculate_haversine_distance(lon1, lat1, lon2, lat2)

                if (distance < 50) and (business_id1 != business_id2):
                    hours_overlap = self.hours_overlap(hours1, hours2)
                    review_count_sim = 0.3 * (((rev_count1 + rev_count2) - 
                        abs(rev_count1 - rev_count2)) / (rev_count1 + rev_count2)) + 0.7
                    category_sim = 0.5 * len(set(categories1).intersection(set(categories2))) \
                        / min(len(categories1), len(categories2)) + 0.5
                    score = 2.718 ** (5 - distance / 10) * ((5 - abs(stars1 - 
                        stars2)) / 5) * review_count_sim * category_sim
                    if hours_overlap > 0:
                        score *= hours_overlap
                    else:
                        score *= 0.75
                    sim_score_total += score

        success_score = stars1 * rev_count1 * vader_sentiment

        yield business_id1, str(sim_score_total) + '\t' + str(success_score)



if __name__ == '__main__':
    MRScores.run()
