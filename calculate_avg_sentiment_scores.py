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


    def mapper_init(self):
        self.sia = SentimentIntensityAnalyzer()


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
        review = next(csv.reader([line]))
        business_id = review[2]
        text = review[5]
        yield business_id, self.sia.polarity_scores(text)['compound']


    def combiner(self, business_id, scores):
        '''
        Combiner function. Takes in key value pairs of a business id and
        scores for that business and yields the sum and count of scores.

        Inputs:
            self: an instance of the MRSentimentScores class
            business_id (str): the business's id, the key of the pair
            scores (generator): the scores for the business, the value of the pair

        Yield:
            A key value pair of the business id and a tuple of the sum and count
            of scores for the business within this combiner
        '''
        total = 0
        count = 0

        for score in scores:
            total += score
            count += 1

        yield business_id, (total, count)


    def reducer(self, business_id, totals_and_counts):
        '''
        Reducer function. Takes in key value pairs of a business id and scores 
        and averages the scores

        Inputs:
            self: an instance of the MRSentimentScores class
            business_id (str): the business's id, the key of the pair
            totals_and_counts (generator): the totals and counts of scores,
                the value of the pair

        Yield:
            A key value pair of the business id and its average sentiment score
        '''
        totals = 0
        counts = 0

        for total, count in totals_and_counts:
            totals += total
            counts += count

        avg_score = totals/counts

        yield business_id, str((avg_score + 1) / 2)


if __name__ == '__main__':
    MRAverageSentimentScores.run()
