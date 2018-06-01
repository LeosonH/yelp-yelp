import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary
#from mr3px.csvprotocol import CsvProtocol

stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
dictionary = Dictionary()
l = list()


class create_base_vector(MRJob):
    #OUTPUT_PROTOCOL = CsvProtocol
    def mapper(self, _, line):
        doc = []
        #review_list = line.split('|')
        review_list = next(csv.reader([line], delimiter = '|'))
        review_text = review_list[-1]
        biz_id = review_list[1]
        # Check for both header rows and blank rows
        #print(review_list)
        if len(re.findall('.*text$', review_text)) == 0 and not re.match(r'^\s*$', review_text):
            doc = re.sub("[^\\w\\s]", "", review_text)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            # remove stop words
            doc = [x for x in doc if x not in stopw]
            doc = list(set(doc))
            yield biz_id, doc 

    #def combiner(self, biz_id, docs):
       #docs = list(docs)
       # docs = [word for word_l in docs for word in word_l]
       # docs = list(set(docs))
       # yield biz_id, docs


    #def reducer_init(self):
        #self.l = list()

    def reducer(self, biz_id, docs):
        docs = list(docs)
        docs = [word for word_l in docs for word in word_l]
        docs = list(set(docs))
        #print(docs)
        #self.l.append(docs)
        yield None, docs


    def reducer_more(self, _, docs):
        docs = list(docs)
        yield None, docs 

    def steps(self):
        return[
                MRStep(mapper = self.mapper,
                    reducer = self.reducer),
                MRStep(reducer = self.reducer_more)

        ]


    #def reducer_final(self):
        #yield None, list(self.l)

    	# return generator to list form
        
        #for i in self.l:
        	#dictionary.add_documents(i)
        # replace directory path with your own
        #dictionary.save("/Users/Nancygong/Documents/Downloads/Nancy Gong/芝加哥/CS3/yelp project/dataset/base_vector.dict")
    
    
if __name__ == '__main__':
    create_base_vector.run()
    






