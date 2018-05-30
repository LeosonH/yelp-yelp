# CS123 Spring '18
# Calculate success scores for each business
# Lily Li

# to save output to csv: python3 calculate_success_scores.py data/yelp_business_sample.csv > output.csv
# helpful link: http://jmedium.com/mapreduce-additionalfile/


from mrjob.job import MRJob
from mrjob import protocol
import csv


class MRSuccessScores(MRJob):
    '''
    Class for MapReduce work.
    '''

    OUTPUT_PROTOCOL = protocol.TextProtocol

    def configure_options(self):
        super(MRSuccessScores, self).configure_options()
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
        business_id = business[0]
        stars = business[9]
        review_count = business[10]
        is_open = business[11]
        categories = business[12]
        score = 0
        score2 = 0

        with open('yelp_hours_samp.csv') as f:
            for line in f:
                fields = line.split(',')
                attr_bus_id = fields[0]
                if attr_bus_id == business_id:
                    score = fields[1]
                    break

        with open('yelp_business_attributes_samp.csv') as f:
            for line in f:
                fields = line.split(',')
                attr_bus_id = fields[0]
                if attr_bus_id == business_id:
                    score2 = fields[1]
                    break


        yield business_id, str(score)


if __name__ == '__main__':
    MRSuccessScores.run()
