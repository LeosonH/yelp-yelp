#-------------------------------------------------------------------------------
# Name: compute_similar_users
#
# Author(s): Leoson, Nancy
#
#-------------------------------------------------------------------------------
# Given a pair of datasets of aggregated reviews for each unique user, yield 
# the most similar other user for each user and their similarity scores. The 
# scores are computed using cosine similarity within an LSI vector space.
#
# To run: python3 create_similar_users.py --file reviews_dictionary.dict --file 
# reviews_corpus.mm --file user_vector.txt -r dataproc --instance-type n1-highmem-2 
# --num-core-instances 7 user_vector.txt > similar_user.txt

from mrjob.job import MRJob
import csv
import re
import pandas as pd
import gensim
from gensim.corpora import Dictionary, MmCorpus
from gensim import models, similarities
from gensim.matutils import cossim
import ast 

class compute_similar_users(MRJob):
	def mapper_init(self):
		'''
        Load required files and models here.
        '''
		# load prerequisite document vectors and paired dataset
		self.dictionary = Dictionary.load("reviews_dictionary.dict")
		self.corpus = MmCorpus("reviews_corpus.mm")
		self.df = pd.read_csv("user_vector.txt", sep = '\t', header = None)
		# initialize lsi space
		self.lsi = models.LsiModel(self.corpus, id2word = self.dictionary, num_topics=8)
	
	def mapper(self, _, line):
		user_v = next(csv.reader([line], delimiter = "\t"))
		user = user_v[0]
		# strip unneeded symbols
		user.strip("\"")
		user_vec = ast.literal_eval(user_v[1])
		query = self.lsi[user_vec]

		for i in range(len(self.df)):
			user_id =  self.df.iloc[i][0]
			if user_id != user:
				compare_vec = ast.literal_eval(self.df.iloc[i][1])
				compare_lsi = self.lsi[compare_vec]
				sim = cossim(query, compare_lsi)
				yield user, (sim, user_id)

	def reducer(self, user, sims):
		sims = list(sims)
		yield user, max(sims, key = lambda item:item[0])[1]


if __name__ == '__main__':
    compute_similar_users.run()

