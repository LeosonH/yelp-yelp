# CS123 Spring '18
# Calculate NLTK scores for each review
# Lily Li

# python3 calculate_sentiment_scores.py data/subset.csv > data.txt

from mrjob.job import MRJob
from mrjob import protocol
import csv
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class MRSentimentScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    OUTPUT_PROTOCOL = protocol.TextProtocol


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
        business_id = business[0]
        text = business[5]
        yield business[0], self.sia.polarity_scores(text)['compound']


if __name__ == '__main__':
    MRSentimentScores.run()
