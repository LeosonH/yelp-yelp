from mrjob.job import MRJob
import csv
import re
import pandas as pd
import gensim
from gensim.corpora import Dictionary, MmCorpus
from gensim import models, similarities
from gensim.matutils import cossim

# load prerequisite document vectors and paired dataset
dictionary = Dictionary.load("base_vector.dict")
corpus = MmCorpus("corpus_vector.mm")
df = pd.read_csv("vm_test.csv")

# initialize lsi space
lsi = models.LsiModel(corpus, id2word=dictionary, num_topics=15)

class compute_similar_users(MRJob):
	'''
	Given a pair of datasets of aggregated reviews for each unique user, yield 
	the most similar other user for each user and their similarity scores.

	The scores are computed using cosine similarity within an LSI vector space.
	'''
	def mapper(self, _, line):
		self.l = list()
		read_line = next(csv.reader([line]))
		user = read_line[0]
		user_text = read_line[1]
		text_bow = dictionary.doc2bow(user_text.lower().split())
		query = lsi[text_bow]

		for i in range(len(df)):
			if pd.isnull(df.iloc[i][1]) == False and df.iloc[i][0] != user:
				comparison = df.iloc[i][1].lower().split()
				vec_bow = dictionary.doc2bow(comparison)
				vec_lsi = lsi[vec_bow]
				sim = cossim(query, vec_lsi)
				self.l.append((sim, df.iloc[i][0]))

		yield max(self.l, key = lambda item:item[0]), user

if __name__ == '__main__':
    compute_similar_users.run()

