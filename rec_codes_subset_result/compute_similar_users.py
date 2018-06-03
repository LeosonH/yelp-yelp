from mrjob.job import MRJob
import csv
import re
import pandas as pd
import gensim
from gensim.corpora import Dictionary, MmCorpus
from gensim import models, similarities
from gensim.matutils import cossim
import ast 

# load prerequisite document vectors and paired dataset
dictionary = Dictionary.load("biz_review_sub.dict")
corpus = MmCorpus("user_rest.mm")
df = pd.read_csv("user_vector.txt", sep = '\t', header = None)
# initialize lsi space
lsi = models.LsiModel(corpus, id2word=dictionary, num_topics=15)

class compute_similar_users(MRJob):
	'''
	Given a pair of datasets of aggregated reviews for each unique user, yield 
	the most similar other user for each user and their similarity scores.

	The scores are computed using cosine similarity within an LSI vector space.
	'''




	def mapper(self, _, line):
		#self.l = list()
		user_v = next(csv.reader([line], delimiter = "\t"))
		user = user_v[0]
		user.strip("\"")
		user_vec = ast.literal_eval(user_v[1])
		query = lsi[user_vec]

		
		for row in df.itertuples():
			user_id =  row._1
			if user_id != user:
				compare_vec = ast.literal_eval(row._2)
				compare_lsi = lsi[compare_vec]
				sim = cossim(query, compare_lsi)
				yield user, (sim, user_id)
	



	def reducer(self, user, sims):
		sims = list(sims)
		yield user, max(sims, key = lambda item:item[0])[1]


if __name__ == '__main__':
    compute_similar_users.run()

