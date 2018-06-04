#-------------------------------------------------------------------------------
# Name: create_user_vector
#
# Author: Nancy
#
#-------------------------------------------------------------------------------
# Takes a base dictionary and a dataset of reviews, create a vectorized representation
# of each user's reviews.
#
# To run: python3 create_user_vector.py --file reviews_dictionary.dict -r dataproc 
# [REVIEWS FILENAME] > user_vector.txt

from mrjob.job import MRJob
import csv
import re
import pandas as pd
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
from gensim.corpora import Dictionary

# create list of stop words
stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])

class create_user_vector(MRJob):

	def mapper(self, _, line):
		review_list = list(next(csv.reader([line], delimiter = '|')))
		# change the index in the list depending on the location of variable in the data
		user_id = review_list[1]
		review_text = review_list[-1]
		if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
			# remove unneeded symbols
			doc = re.sub("[^\\w\\s]", "", review_text)
			doc = re.sub(r"\b\d+\b","", doc)
			doc = doc.lower().split()
			doc = [x for x in doc if x not in stopw] 
			yield user_id, doc 

	def reducer_init(self):
		'''
		Load required files here.
		'''
		# load dictionary
		self.dictionary = Dictionary.load("reviews_dictionary.dict")

	def reducer(self, user_id, docs):
		text = [word for doc in docs for word in doc]
		vec = self.dictionary.doc2bow(text)
		vec = list(vec)
		yield user_id, vec 


if __name__ == '__main__':
    create_user_vector.run()