from mrjob.job import MRJob
import csv
import re
import pandas as pd
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
from gensim.corpora import Dictionary, MmCorpus


stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
dictionary = Dictionary.load("biz_review_sub.dict")

class create_user_rest_pair(MRJob):
	def mapper(self, _, line):
		review_list = list(next(csv.reader([line], delimiter = '|')))
		'''
		change the index in the list depending 
		on the location of variable in the data
		'''
		rest_id = review_list[1]
		user_id = review_list[3]
		review_text = review_list[-1]
		la = review_list[6]
		lon = review_list[7]
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
		vec = dictionary.doc2bow(words)
		vec = list(vec)
		for i in info:
			yield (i[0], rest_id, i[1], i[2]), vec


if __name__ == '__main__':
	create_user_rest_pair.run()
