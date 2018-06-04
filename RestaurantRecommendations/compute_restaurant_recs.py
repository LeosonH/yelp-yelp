#-------------------------------------------------------------------------------
# Name: compute_restaurant_recs
#
# Author: Leoson, Nancy
#
#-------------------------------------------------------------------------------
# Given a dataset of user-pairs and recommendable restaurants, compute the recommendability score
# for each restaurant and output the most recommendable one.
#
# To run: python3 compute_restaurant_recs.py -r dataproc --num-core-instances 7 user_sim_res_pair.txt 
# > result.txt

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
from mrjob.step import MRStep
import ast

class compute_recommendations(MRJob):
	def mapper(self, _, line):
		read_line = next(csv.reader([line], delimiter = "\t"))
		info = ast.literal_eval(read_line[1])
		user = info[0]
		sim_user = info[1]
		rest = info[2]
		sim_rest = info[3]
		sim_score= info[4]
		dist = info[5]
		
		yield (user,sim_user,sim_rest), (sim_score, dist)

	def reducer_inter(self, info, value):
		user = info[0]
		sim_user = info[1]
		sim_rest = info[2]
		sim_list = list()
		dist_list = list()
		value = list(value)
		for t in value:
			sim_list.append(t[0])
			dist_list.append(t[1])
		sim_mean = np.mean(sim_list)
		dist_mean = np.mean(dist_list)
		log_dist_mean = np.log(dist_mean)
		# applies score formula
		score = sim_mean/log_dist_mean
		yield (user, sim_user), (score, sim_rest)

	def reducer_final(self, info, value):
		user = info[0]
		sim_user = info[1]
		value = list(value)
		value.sort(reverse = True)
		yield(user, sim_user), value[0]

	def steps(self):
		return[
				MRStep(mapper = self.mapper,
					#reducer_init = self.reducer_init,
					reducer = self.reducer_inter),
				MRStep(reducer = self.reducer_final)
		]
	


if __name__ == '__main__':
    compute_recommendations.run()

