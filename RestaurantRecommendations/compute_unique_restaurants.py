#-------------------------------------------------------------------------------
# Name: compute_unique_restaurants
#
# Author(s): Leoson, Nancy
#
#-------------------------------------------------------------------------------
# Given a dataset of user-paired restaurants, and a dataset of paired most-similar users, 
# find potential recommendable restaurants for each user and their similarity score and haversine
# distance.
#
# To run: python3 compute_unique_restaurants.py -r dataproc --file user_rest_pair.csv 
# --num-core-instances 7 similar_users.txt > user_sim_rest_pair.txt

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
	def mapper_init(self):
		'''
        Load required files and models here.
        '''
		# load prerequisite document vectors and paired dataset
		self.dictionary = Dictionary.load("reviews_dictionary.dict")
		self.corpus = MmCorpus("reviews_corpus.mm")
		self.df = pd.read_csv("user_rest_pair.csv", sep = "|")
		# initialize lsi space
		self.lsi = models.LsiModel(self.corpus, id2word = self.dictionary, num_topics=15)

	def mapper(self, _, line):
		read_line  = list(next(csv.reader([line], delimiter = "\t")))
		user = read_line[0]
		sim_user = read_line[1]

		user_df = self.df[self.df["user"] == user]
		user_rest = list(user_df["rest"].values)
		sim_user_df = self.df[(self.df["user"] == sim_user)& (~self.df["rest"].isin(user_rest))]

		for i in range(len(user_df)):
			rest = user_df.iloc[i]["rest"]
			la = float(user_df.iloc[i]["la"])
			lon = float(user_df.iloc[i]["lon"])
			# strip unneeded symbols
			vec  = ast.literal_eval(user_df.iloc[i]["vec"])
			lsi1 = self.lsi[vec]
			for j in range(len(sim_user_df)):
				sim_rest = sim_user_df.iloc[i]["rest"]
				sim_la= float(sim_user_df.iloc[i]["la"])
				sim_lon = float(sim_user_df.iloc[i]["lon"])
				sim_vec  = ast.literal_eval(sim_user_df.iloc[i]["vec"])
				lsi2 = self.lsi[sim_vec]
				sim_score = cossim(lsi1, lsi2)
				dist = haversine_distance((la, lon), (sim_la, sim_lon))
				yield None, (user, sim_user, rest, sim_rest, sim_score, dist)

if __name__ == '__main__':
    compute_unique.run()
