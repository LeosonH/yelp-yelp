#-------------------------------------------------------------------------------
# Name: create_vector
#
# Author: Leoson, Nancy
#
#-------------------------------------------------------------------------------
# Takes the review text in a dataset of yelp reviews, and converts it into a list of 
# individual useful words to build a dictionary.
#
# To run: python3 create_vector.py -r dataproc --num-core-instances 7 [REVIEWS FILENAME] > dict.txt

import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re

# create list of stop words
stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])

class create_base_vector(MRJob):
    def mapper(self, _, line):
        doc = []
        review_list = next(csv.reader([line], delimiter = '|'))
        # change indexes as necessary
        review_text = review_list[-1]
        biz_id = review_list[0]
        # check for null rows and spaces
        if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
            # remove irrelevant symbols
            doc = re.sub("[^\\w\\s]", "", review_text)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            # remove stop words
            doc = [x for x in doc if x not in stopw]
            doc = list(set(doc))
            yield biz_id, doc 

    def reducer(self, biz_id, docs):
        docs = list(docs)
        docs = [word for word_l in docs for word in word_l]
        docs = list(set(docs))
        yield None, docs

    def reducer_more(self, _, docs):
        docs = list(docs)
        yield None, docs 

    def steps(self):
        return[
                MRStep(mapper = self.mapper,
                    reducer = self.reducer),
                MRStep(reducer = self.reducer_more)
        ]
    
if __name__ == '__main__':
    create_base_vector.run()
    
