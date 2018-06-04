## Pair-based Restaurant Recommendations

### Description:
Outputs restaurant recommendations for users in the dataset.

### Process Documentation:

### I. Prepare Dictionary and Corpus for Gensim:

```sh
python3 create_vector.py -r dataproc --num-core-instances 7 [REVIEWS FILENAME] > dict.txt
```
This creates a list formatted file of words from a set of Yelp Reviews. Use **txt_to_dict.ipynb** to clean and convert this to reviews_dictionary.dict. This builds the gensim dictionary.

```sh
python3 create_corpus.py -r dataproc --file reviews_dictionary.dict --num-core-instances 7 [REVIEWS FILENAME] > corpus.txt
```
This creates a list formatted file of tuples from a set of Yelp Reviews. Use **txt_to_dict.ipynb** to clean and convert this to reviews_corpus.mm. This builds the gensim corpus.

### II. Create vectorized representation of users and compute most similar users:
