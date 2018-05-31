from mrjob.job import MRJob
import csv
import re
import pandas as pd


review_restaurant_df = pd.read_csv("vm_test2.csv")

class compute_unique(MRJob):
	'''
	Given a dataset of reviews, and a dataset of paired most-similar users, 
	find potential recommendable restaurants for each user.

	Yields the user's id, the most similar user's id, and a recommendable 
	restaurant from the latter.
	'''
	def mapper(self, _, line):
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
				yield (user, sim_user), (j, k)

if __name__ == '__main__':
    compute_unique.run()
