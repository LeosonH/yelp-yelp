import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary, MmCorpus

stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
dictionary = Dictionary.load("base_vector.dict")
class create_corpus(MRJob):
    
    def mapper(self, _, line):
        doc = []
        vec = []
        review_list = line.split(',')
        review_text = review_list[-1]
        if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
            doc = re.sub("[^\\w\\s]", "", review_text)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            doc = [x for x in doc if x not in stopw]
            vec = dictionary.doc2bow(doc)
            yield None, vec        

    def reducer_init(self):
        self.corpus = list()

    def reducer(self, _, vec):
        for i in list(vec):
            self.corpus.append(i)
        # replace directory path with your own
        MmCorpus.serialize("C:/Users/leoso/Desktop/Uchicago Year 1/Spring/git/yelp-yelp/corpus_vector.mm", self.corpus)
    
if __name__ == '__main__':
    create_corpus.run()
    






