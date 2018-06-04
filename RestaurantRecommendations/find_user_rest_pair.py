#-------------------------------------------------------------------------------
# Name: find_user_rest_pair
#
# Author: Nancy
#
#-------------------------------------------------------------------------------
# Given a base dictionary and a dataset of reviews, create a dataset that maps
# each restaurant to users. 
#
# To run: python3 find_user_rest_pair.py --file reviews_dictionary.dict -r dataproc 
# --num-core-instances 7 [REVIEWS FILENAME] > user_rest_pair.txt

from mrjob.job import MRJob
import csv
import re
import pandas as pd
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
from gensim.corpora import Dictionary, MmCorpus

# create list of stop words
stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])


class create_user_rest_pair(MRJob):
	def mapper_init(self):
		'''
		Load necessary files here.
		'''
		# load dictionary
		self.dictionary = Dictionary.load("reviews_dictionary.dict")

	def mapper(self, _, line):
		review_list = list(next(csv.reader([line], delimiter = '|')))
		# change the index in the list depending on the location of variable in the data
		rest_id = review_list[0]
		user_id = review_list[1]
		review_text = review_list[-1]
		la = review_list[-2]
		lon = review_list[-3]
		if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
			doc = re.sub("[^\\w\\s]", "", review_text)
			doc = re.sub(r"\b\d+\b","", doc)
			doc = doc.lower().split()
			doc = [x for x in doc if x not in stopw] 
			yield rest_id, (user_id,la, lon, doc)

	def reducer(self, rest_id, info):
		info = list(info)
		word_list = [t[3] for t in info]
		words = [word for doc in word_list for word in doc]
		vec = self.dictionary.doc2bow(words)
		vec = list(vec)
		for i in info:
			yield (i[0], rest_id, i[1], i[2]), vec


if __name__ == '__main__':
	create_user_rest_pair.run()
