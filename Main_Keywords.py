from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
from elasticsearch import helpers
import json
from datetime import datetime, timedelta
import glob
import time
import sys
from datetime import datetime,timedelta
from datetime import time
from datetime import date
from SearchAPI import NewsVolumeQuery,APISearch
from NewsVolumeIndicator import NewsVolumeIndicator
from NewsStatistics import StatsDataStructure
from NewsVolumeStatistics import NewsVolumeStatistics
from Screeners import Screener
from GenerateDataset import GenerateDataset
from IPython.display import HTML
from datetime import date, time, datetime,timedelta
from CONFIG import configFile

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth',None)

date = datetime.now()
formattedDateNow = date.strftime('%Y-%m-%dT%H-%M-%S')

CONFIG = configFile()

with open(r"E:\Data\Keywords\keywords.txt", 'r') as file:
    keywords = (file.readlines())

    for i,item in enumerate(file):
        keywords[i] = item.replace("\n","")

es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)
indexes = ['news','tweets']

if __name__ == '__main__':
    for index in indexes:
        text_file = open(rf"E:\Data\Keywords\keywords_{index}_{formattedDateNow}.html", "w")

        for keyword in keywords:
            apisearch = APISearch()

            if index == 'news':
                query = (APISearch.search_news(apisearch, search_string=f"{keyword}", timestamp_from='now-3d', timestamp_to='now'))
                res = es_client.search(index=index, body=query, size=10000)
                df = GenerateDataset(index)

                dfTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])
                dfTicker = (dfTicker.drop_duplicates(subset=['title']))
                dfTicker = dfTicker.loc[:, ['published_datetime', 'title','tickers']]

                if len(dfTicker != 0):
                    text_file.write(
                        "*****************************************************************************************************")
                    text_file.write("<br>")
                    text_file.write("<br>")
                    text_file.write("")

                    text_file.write("Keyword:         %s" % (keyword))
                    text_file.write("<br>")
                    # print("Ticker:         %s" % (ticker))
                    try:
                        text_file.write(dfTicker.to_html(classes='table table-striped'))
                    except Exception as e:
                        pass

                text_file.write("<br>")

            elif index =='tweets':
                query = (APISearch.search_tweets(apisearch, search_string=f"{keyword}", timestamp_from='now-3d',timestamp_to='now'))
                res = es_client.search(index=index, body=query, size=10000)
                df = GenerateDataset(index)

                dfTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])
                dfTicker = (dfTicker.drop_duplicates(subset=['tweet']))
                dfTicker = dfTicker.loc[:, ['timestamp', 'tweet', 'cashtags']]

                if len(dfTicker != 0):
                    text_file.write(
                        "*****************************************************************************************************")
                    text_file.write("<br>")
                    text_file.write("<br>")
                    text_file.write("")

                    text_file.write("Keyword:         %s" % (keyword))
                    text_file.write("<br>")
                    # print("Ticker:         %s" % (ticker))
                    try:
                        text_file.write(dfTicker.to_html(classes='table table-striped'))
                    except Exception as e:
                        pass

                text_file.write("<br>")

        text_file.close()