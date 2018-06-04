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

```sh
python3 create_user_vector.py --file reviews_dictionary.dict -r dataproc [REVIEWS FILENAME] > user_vector.txt
```
This creates a tab delimited file of users and their vectorized reviews.

```sh
python3 create_similar_users.py --file reviews_dictionary.dict --file reviews_corpus.mm --file user_vector.txt -r dataproc --instance-type n1-highmem-2 --num-core-instances 7 user_vector.txt > similar_user.txt
```
This takes in user_vector.txt and creates a tab delimited file of users and their most similar users, along with the similarity scores.

### III. Pair up users and restaurants, and compute scores and distances for recommendable restaurants:

```sh
python3 find_user_rest_pair.py --file reviews_dictionary.dict -r dataproc --num-core-instances 7 [REVIEWS FILENAME] > user_rest_pair.txt
```
This creates a tab delimited file that maps users to the restaurants they have visited. Use **user_rest_pair_csv.ipynb** to convert the output into csv file.

```sh
python3 compute_unique_restaurants.py -r dataproc --file user_rest_pair.csv --num-core-instances 7 similar_users.txt > user_sim_rest_pair.txt
```
This takes in user_rest_pair.csv and similar_users.txt to create unique restaurant pairs between each user and their most similar user, along with the similarity and distance scores.

### IV. Compute recommendations:

