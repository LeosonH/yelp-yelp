# CS123 Spring '18
# Calculate success scores for each business
# Lily Li

# to save output to csv: python3 calculate_success_scores.py --jobconf mapreduce.job.reduces=1 <data/yelp_business_sample.csv> output.csv
# to print output to terminal: python3 calculate_success_scores.py --jobconf mapreduce.job.reduces=1 --items data/yelp_business_success_scores_sample.csv --items2 data/yelp_business_similarity_scores_sample.csv data/yelp_business_sample.csv
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

    def configure_options(self):
        super(MRSuccessScores, self).configure_options()
        self.add_file_option('--businesses', help='path to yelp_business_success_scores_sample.csv')


    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and pairs it
        with all other business in the file

        Inputs:
            self: an instance of the MRSuccessScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key value pair of the person's name and their status
        '''
        business = next(csv.reader([line]))
        # first file to be processed must be "main" file (i.e. not a file in add_file_option())
        business_id = business[0]
        stars = business[9]
        review_count = business[10]
        is_open = business[11]
        categories = business[12]
        score = 0
        score2 = 0

        with open('yelp_business_success_scores_sample.csv') as f:
            for line in f:
                fields = line.split(',')
                attr_bus_id = fields[0]
                if attr_bus_id != business_id:
                    score = fields[1]
                    break


        yield business, (score, score2)


if __name__ == '__main__':
    MRSuccessScores.run()
