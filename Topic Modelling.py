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

def transformerModel(data,path_to_model,seq_length):

    word_embedding_model = models.Transformer(path_to_model, max_seq_length=seq_length)
    pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
    model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    embeddings = model.encode(data)

    return embeddings

def umapEmbeddings(neighbors,components,metric,embeddings):
    umap_embeddings = umap.UMAP(n_neighbors=neighbors, # 15
                                n_components=components, #5 cosine
                                metric=metric).fit_transform(embeddings)
    return umap_embeddings

def topicClusters(cluster_size, metric,cluster_selection_method,embeddings):
    cluster = hdbscan.HDBSCAN(min_cluster_size=cluster_size, # 5,euclidean,eom,umap_embeddings
                              metric=metric,
                              cluster_selection_method=cluster_selection_method).fit(embeddings)
    return cluster


def c_tf_idf(documents, m, ngram_range=(3, 3)):
    """
    ngram_range(x,y): indicates the ngram to find within topics
    """

    count = CountVectorizer(ngram_range=ngram_range, stop_words="english").fit(documents)
    t = count.transform(documents).toarray()
    w = t.sum(axis=1)
    tf = np.divide(t.T, w)
    sum_t = t.sum(axis=0)
    idf = np.log(np.divide(m, sum_t)).reshape(-1, 1)
    tf_idf = np.multiply(tf, idf)

    return tf_idf, count

def extract_top_n_words_per_topic(tf_idf, count, docs_per_topic, n=20):
    words = count.get_feature_names()
    labels = list(docs_per_topic.Topic)
    tf_idf_transposed = tf_idf.T
    indices = tf_idf_transposed.argsort()[:, -n:]
    top_n_words = {label: [(words[j], tf_idf_transposed[i][j]) for j in indices[i]][::-1] for i, label in enumerate(labels)}

    return top_n_words

def extract_topic_sizes(df):
    topic_sizes = (df.groupby(['Topic'])
                     .Doc
                     .count()
                     .reset_index()
                     .rename({"Topic": "Topic", "Doc": "Size"}, axis='columns')
                     .sort_values("Size", ascending=False))

    return topic_sizes


if __name__ == '__main__':

    # Parameters #
    path_to_model = r'E:\Pretrained Models\distilbert-base-nli-stsb-mean-tokens'
    index = 'news'
    keyword = 'crude oil'
    start_time = 'now-10d'
    end_time = 'now'

    # Generate Dataframe and join all titles and preprocess #
    dfTicker = themedf(index, keyword, start_time, end_time)
    cleanSentence = []

    for title in dfTicker.title:
        cleanSentence.append(remove_stopwords(textPreprocess(title)))

    cleanSentence = (list(set(cleanSentence)))
    data = cleanSentence


    # Model Parameters #
    embeddings = transformerModel(data, path_to_model, 256)
    umap_embeddings = umapEmbeddings(15, 5, 'cosine',embeddings)
    cluster = topicClusters(5, 'euclidean', 'eom', umap_embeddings)

    umap_data = umap.UMAP(n_neighbors=15, n_components=2, min_dist=0.0, metric='cosine').fit_transform(embeddings)
    result = pd.DataFrame(umap_data, columns=['x', 'y'])
    #umap_embeddings = umapEmbeddings(n_neighbors = 15, n_components = 5, metric = 'cosine').fit_transform(embeddings)
    result['labels'] = cluster.labels_

    docs_df = pd.DataFrame(data, columns=["Doc"])
    docs_df['Topic'] = cluster.labels_
    docs_df['Doc_ID'] = range(len(docs_df))
    docs_per_topic = docs_df.groupby(['Topic'], as_index=False).agg({'Doc': ' '.join})

    tf_idf, count = c_tf_idf(docs_per_topic.Doc.values, m=len(data))


    top_n_words = extract_top_n_words_per_topic(tf_idf, count, docs_per_topic, n=20)
    topic_sizes = extract_topic_sizes(docs_df)

    #print(top_n_words)

    topic_number = []
    keywords_topics = []
    tfScore = []
    n = 10

    for key in list(top_n_words.keys()):

        if key != -1:
            for j, element in enumerate(top_n_words[key][:n]):
                if element[1] > 0.01:
                    if j != n:
                        topic_number.append(key)
                        keywords_topics.append(element[0])
                        tfScore.append(element[1])

    #print(topic_number),print(keywords_topics),print(tfScore)

    df = pd.DataFrame()
    df['topic'] = topic_number
    df['keywords'] = keywords_topics
    df['score'] = tfScore
    df = df.sort_values(by=['topic']).reset_index(drop=True)
    #print(df)

    scores = list(df.groupby('topic').mean().score)
    topicNumber = []
    aggregateTopic = []

    for topic in list(set(df.topic)):
        topicAgg = " ".join(list(set(" ".join(list(df[df.topic == topic].keywords)).split())))
        topicNumber.append(topic)
        aggregateTopic.append(topicAgg)

    aggregatedf = pd.DataFrame()
    aggregatedf['topic'] = topicNumber
    aggregatedf['topic keywords'] = aggregateTopic
    aggregatedf['topic density'] = scores

    aggregatedf = aggregatedf.sort_values(by=['topic density'], ascending=False).reset_index(drop=True)

    print(aggregatedf)

