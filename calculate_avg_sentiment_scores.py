# CS123 Spring '18
# Calculate NLTK scores for each review
# Lily Li


from mrjob.job import MRJob
from mrjob import protocol
import csv
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class MRAverageSentimentScores(MRJob):
    '''
    Class for MapReduce work.
    '''
    OUTPUT_PROTOCOL = protocol.TextProtocol


    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and extracts the 
        business id and review sentiment score.

        Inputs:
            self: an instance of the MRAverageSentimentScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key value pair of the business id and the review's sentiment score
        '''
        row = next(csv.reader([line]))
        business_id = row[0]
        score = row[1]
        yield business_id, score


    def combiner(self, business_id, scores):
        '''
        Combiner function. Takes in key value pairs of a business id and 
        scores for that business and yields the sum and count of scores.

        Inputs:
            self: an instance of the MRGuestsTenTimes class
            name (str): the guest's name, the key of the pair
            scores (generator): the scores for the guest, the value of the pair

        Yield:
            A key value pair of the guest's name and the sum of the scores for
            him/her within this combiner
        '''
        total = 0
        count = 0

        for score in scores:
            total += score
            count += 1

        yield name, (total, count)


    def reducer(self, business_id, totals_and_counts):
        totals = 0
        counts = 0

        for total, count in totals_and_counts:
            totals += total
            counts += count

        yield business_id, totals/counts


if __name__ == '__main__':
    MRAverageSentimentScores.run()
