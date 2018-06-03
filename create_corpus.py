#-------------------------------------------------------------------------------
# Name: create_corpus
#
# Author: Leoson, Nancy
#
#-------------------------------------------------------------------------------
# Given a dictionary and a dataset of yelp reviews, creates a corpus - a frequency
# vector of significant word occurences to use as the vector space for documment
# comparisons.
#
# To run: python3 final_mrjob_scores.py -r dataproc --num-core-instances 7 biz_review_cleaned.csv > dict.txt

import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary, MmCorpus

# create list of stop words
stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])

# load dictionary
dictionary = Dictionary.load("reviews_dictionary.dict")

class create_corpus(MRJob):
    def mapper(self, _, line):
        review_list = list(next(csv.reader([line], delimiter = '|')))
        review_text = review_list[1]
        # review_info = [review_list[1], review_list[6], review_list[7]]
        # ind_info = " ".join(review_info)
        if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
            # remove irrelevant symbols
            doc = re.sub("[^\\w\\s]", "", review_text)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            # remove stop words
            doc = [x for x in doc if x not in stopw] 

            yield None, doc

    def reducer(self, _, docs):
        text = [word for doc in docs for word in doc]
        vec = dictionary.doc2bow(text)
        vec = list(vec)

        yield None, vec

    def reducer_more(self, _, vec):
        vec = list(vec)
        yield None, vec

    def steps(self):
        return[
                MRStep(mapper = self.mapper,
                    reducer = self.reducer),
                MRStep(reducer = self.reducer_more)

        ]
    
if __name__ == '__main__':
    create_corpus.run()
    
