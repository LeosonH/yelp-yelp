from mrjob.job import MRJob
import csv
import re
import pandas as pd
import gensim
import math
from gensim.corpora import Dictionary, MmCorpus
from gensim import models, similarities
from gensim.matutils import cossim
import numpy as np
import ast
#review_restaurant_df = pd.read_csv("vm_test2.csv")

dictionary = Dictionary.load("biz_review_sub.dict")
corpus = MmCorpus("user_rest.mm")
df = pd.read_csv("user_rest_pair.csv", sep = "|")

lsi = models.LsiModel(corpus, id2word=dictionary, num_topics=15)

def haversine_distance(restaurant1, restaurant2):
	'''
	Calculates the haversine distance between restaurants, given a latitude and
	a longtitude.
	'''
	lat1, lon1 = restaurant1
	lat2, lon2 = restaurant2
	radius = 6371 # km

	dlat = math.radians(lat2-lat1)
	dlon = math.radians(lon2-lon1)
	a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
		* math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
	d = radius * c

	return d


class compute_unique(MRJob):
	'''
	Given a dataset of reviews, and a dataset of paired most-similar users, 
	find potential recommendable restaurants for each user.

	Yields the user's id, the most similar user's id, and a recommendable 
	restaurant from the latter.
	'''
	def mapper(self, _, line):
		read_line  = list(next(csv.reader([line], delimiter = "\t")))
		user = read_line[0]
		sim_user = read_line[1]

		user_df = df[df["user"] == user]
		user_rest = list(user_df["rest"].values)
		sim_user_df = df[(df["user"] == sim_user)& (~df["rest"].isin(user_rest))]

		for row1 in user_df.itertuples():
			rest = row1.rest
			la = float(row1.la)
			lon = float(row1.lon)
			vec  = ast.literal_eval(row1.vec)
			lsi1 = lsi[vec]
			for row2 in sim_user_df.itertuples():
				sim_rest = row2.rest
				sim_la= float(row2.la)
				sim_lon = float(row2.lon)
				sim_vec  = ast.literal_eval(row2.vec)
				lsi2 = lsi[sim_vec]
				sim_score = cossim(lsi1, lsi2)
				dist = haversine_distance((la, lon), (sim_la, sim_lon))
				yield None, (user, sim_user, rest, sim_rest, sim_score, dist)






		'''
		self.s = set()
		self.s2 = set()
		read_line = next(csv.reader([line]))
		sim_user = read_line[1]
		user = read_line[2]

		for i in range(len(review_restaurant_df)):
			if pd.isnull(review_restaurant_df.iloc[i]["text"]) == False:
				if review_restaurant_df.iloc[i]["user_id"] == user:
					self.s.add(review_restaurant_df.iloc[i][1])
				if review_restaurant_df.iloc[i]["user_id"] == sim_user:
					self.s2.add(review_restaurant_df.iloc[i][1])

		# remove overlapping restaurants
		l3 = [x for x in list(self.s2) if x not in list(self.s)]

		for j in l3:
			for k in self.s:
				yield (user, sim_user), (k, j)
		'''

if __name__ == '__main__':
    compute_unique.run()
