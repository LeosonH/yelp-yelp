# CS123 Spring '18
# Calculate NLTK scores for each review
# Lily Li


from mrjob.job import MRJob
from mrjob import protocol
import csv
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class MRSentimentScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    OUTPUT_PROTOCOL = protocol.TextProtocol
    sia = SentimentIntensityAnalyzer()


    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and calculates the
        sentiment score for the review.

        Inputs:
            self: an instance of the MRSentimentScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key value pair of the business id and the review's sentiment score
        '''
        business = next(csv.reader([line]))
        business_id = business[2]
        text = business[5]
        yield business_id, sia.polarity_scores(text)['compound']


if __name__ == '__main__':
    MRSentimentScores.run()
