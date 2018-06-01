import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary, MmCorpus
from mr3px.csvprotocol import CsvProtocol

stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
dictionary = Dictionary.load("biz_review_sub.dict")
class create_corpus(MRJob):
    #OUTPUT_PROTOCOL = CsvProtocol
    def mapper(self, _, line):
        review_list = list(next(csv.reader([line], delimiter = '|')))
        review_text = review_list[-1]
        review_info = [review_list[1], review_list[6], review_list[7]]
        ind_info = " ".join(review_info)
        if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
            doc = re.sub("[^\\w\\s]", "", review_text)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            doc = [x for x in doc if x not in stopw] 

            yield ind_info, doc

    #def reducer_init(self):
        #self.corpus = list()

    def reducer(self, ind_info, docs):
        #for i in list(vec):
            #self.corpus.append(i)
        # replace directory path with your own
        #key = ind_info
        text = [word for doc in docs for word in doc]
        vec = dictionary.doc2bow(text)
        vec = list(vec)

        yield None, vec

    def reducer_more(self, _, vec):
        vec = list(vec)
        yield None, vec

    def steps(self):
        return[
                MRStep(mapper = self.mapper,
                    reducer = self.reducer),
                MRStep(reducer = self.reducer_more)

        ]
      
    
if __name__ == '__main__':
    create_corpus.run()
    






