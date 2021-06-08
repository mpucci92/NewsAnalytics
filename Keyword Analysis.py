import re
import numpy as np
import pandas as pd
from pprint import pprint
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel
import spacy
import matplotlib.pyplot as plt
import string
from nltk.corpus import stopwords
import nltk
from nltk.util import ngrams
from elasticsearch import Elasticsearch
from textPreprocessing import *
from SearchAPI import *
from CONFIG import configFile
from GenerateDataset import *
from gensim.parsing.preprocessing import remove_stopwords
from sentence_transformers import SentenceTransformer, models
import umap
import hdbscan
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

nltk.download('stopwords')
from nltk.corpus import stopwords
stop_words = stopwords.words('english')

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

CONFIG = configFile()
es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)


def themedf(index,keyword,start_time,end_time):
    apisearch = APISearch()

    if index == 'news':
        query = (APISearch.search_news(apisearch, search_string=keyword, timestamp_from=start_time, timestamp_to=end_time))
        res = es_client.search(index=index, body=query, size=10000)
        df = GenerateDataset(index)

        dfTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])
        dfTicker = (dfTicker.drop_duplicates(subset=['title']))
        dfTicker = dfTicker.loc[:, ['published_datetime', 'title','tickers','sentiment_score']]

    return dfTicker



dfTicker = themedf('news','bitcoin', 'now-1d', 'now')
test = ' '.join(dfTicker.title)

from sentence_transformers import SentenceTransformer, models

model_path = r'E:\Pretrained Models\distilbert-base-nli-stsb-mean-tokens'
word_embedding_model = models.Transformer(model_path, max_seq_length=256)
pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])



from sklearn.feature_extraction.text import CountVectorizer

n_gram_range = (8, 8)
stop_words = "english"

# Extract candidate words/phrases
count = CountVectorizer(ngram_range=n_gram_range, stop_words=stop_words).fit([test])
candidates = count.get_feature_names()

doc_embedding = model.encode([test])
candidate_embeddings = model.encode(candidates)

from sklearn.metrics.pairwise import cosine_similarity

top_n = 20
distances = cosine_similarity(doc_embedding, candidate_embeddings)
keywords = [candidates[index] for index in distances.argsort()[0][-top_n:]]

import numpy as np
import itertools
from sklearn.metrics.pairwise import cosine_similarity

def mmr(doc_embedding, word_embeddings, words, top_n, diversity):

    # Extract similarity within words, and between words and the document
    word_doc_similarity = cosine_similarity(word_embeddings, doc_embedding)
    word_similarity = cosine_similarity(word_embeddings)

    # Initialize candidates and already choose best keyword/keyphras
    keywords_idx = [np.argmax(word_doc_similarity)]
    candidates_idx = [i for i in range(len(words)) if i != keywords_idx[0]]

    for _ in range(top_n - 1):
        # Extract similarities within candidates and
        # between candidates and selected keywords/phrases
        candidate_similarities = word_doc_similarity[candidates_idx, :]
        target_similarities = np.max(word_similarity[candidates_idx][:, keywords_idx], axis=1)

        # Calculate MMR
        mmr = (1-diversity) * candidate_similarities - diversity * target_similarities.reshape(-1, 1)
        mmr_idx = candidates_idx[np.argmax(mmr)]

        # Update keywords & candidates
        keywords_idx.append(mmr_idx)
        candidates_idx.remove(mmr_idx)

    return [words[idx] for idx in keywords_idx]

print(mmr(doc_embedding,candidate_embeddings,candidates,top_n=10,diversity=0.2))
