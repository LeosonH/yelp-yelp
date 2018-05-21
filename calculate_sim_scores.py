# CS123 Spring '18
# Calculate success scores for each business
# Lily Li

# to save output to csv: python3 calculate_success_scores.py --jobconf mapreduce.job.reduces=1 <data/yelp_business_sample.csv> output.csv
# to print output to terminal: python3 calculate_success_scores.py --jobconf mapreduce.job.reduces=1 data/yelp_business_sample.csv
# helpful link: http://jmedium.com/mapreduce-additionalfile/


from mrjob.job import MRJob
from mrjob import protocol
import csv
# from mr3px import csvprotocol
# use this package to output to csv using a CSV protocol

class MRSuccessScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    # OUTPUT_PROTOCOL = csvprotocol.CsvProtocol

    def configure_args(self):
        super(MRSuccessScores, self).configure_args()
        self.add_file_arg('--scoring-db')

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
        business_id = business[0]
        latitude = business[7]
        longitude = business[8]
        stars = business[9]
        review_count = business[10]
        is_open = business[11]
        categories = business[12]

        score = 5

        # yield business, score

# def reducer(self, business, score):
#         '''
#         Reducer function. Takes in key value pairs of a name and statuses and 
#         finds the unique statuses to find those who appeared as both statuses.

#         Inputs:
#             self: an instance of the MRGuestsBothYears class
#             name (str): the person's name, the key of the pair
#             statuses (generator): the statuses for the person, the value of 
#                 the pair

#         Yield:
#             A key value pair of None and the person's name
#         '''
#         yield business, score









if __name__ == '__main__':
    MRSuccessScores.run()
