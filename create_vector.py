import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary

stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
dictionary = Dictionary()


class create_base_vector(MRJob):
    def mapper(self, _, line):
        doc = []
        # Check for both header rows and blank rows
        if len(re.findall('.*text$', line)) == 0 and not re.match(r'^\s*$', line):
            doc = re.sub("[^\\w\\s]", "", line)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            # remove stop words
            doc = [x for x in doc if x not in stopw]
            doc = list(set(doc))
        yield None, doc         

    def reducer_init(self):
        self.l = list()

    def reducer(self, _, doc):
    	# return generator to list form
        self.l.append(list(doc))
        for i in self.l:
        	dictionary.add_documents(i)
        # replace directory path with your own
        dictionary.save("C:/Users/leoso/Desktop/Uchicago Year 1/Spring/git/yelp-yelp/base_vector.dict")
    
    
if __name__ == '__main__':
    create_base_vector.run()
    






