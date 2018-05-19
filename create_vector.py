import gensim
import csv
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.feature_extraction import stop_words
import re
from gensim.corpora import Dictionary
import os 
import tempfile
#TEMP_FOLDER = tempfile.gettempdir()
#print('Folder "{}" will be used to save temporary dictionary and corpus.'.format(TEMP_FOLDER))
# dct = Dictionary(texts)
# dct.add_documents([[],[]])
# dct.doc2bow

stopw = list(stop_words.ENGLISH_STOP_WORDS)
stopw.extend(['yelp', 'got', 'does', 'quite','going','just', 'right'])
class create_base_vector(MRJob):
    def mapper(self, _, line):
        if len(re.findall('.*text$', line)) == 0:
            doc = re.sub("[^\\w\\s]", "", line)
            doc = re.sub(r"\b\d+\b","", doc)
            doc = doc.lower().split()
            doc = [x for x in doc if x not in stopw]
            doc = list(set(doc))
            yield None, doc         

    def reducer_init(self):
        self.l = list()

    def reducer_inter(self, _, doc):
        self.l.append(doc)

    def reducer_final(self):
        #self.dict.save(os.path.join(TEMP_FOLDER, "base_vector.dict"))
        #ield None, self.dict
        dictionary = Dictionary(self.l)
        dictionary.save("base_vector.dict")
        yield None, None


if __name__ == '__main__':
    create_base_vector.run()
    






