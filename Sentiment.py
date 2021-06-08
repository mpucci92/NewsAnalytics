from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta
import glob
import time
import sys
from SearchAPI import NewsVolumeQuery,APISearch
from NewsVolumeIndicator import NewsVolumeIndicator
from NewsStatistics import StatsDataStructure
from NewsVolumeStatistics import NewsVolumeStatistics
from Screeners import Screener
from GenerateDataset import GenerateDataset
from datetime import date, time, datetime,timedelta
from CONFIG import configFile
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth',None)

CONFIG = configFile()
es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)
indexes = ['news','tweets']
dt = datetime.now()
dateNow = dt.date()
timeNow = datetime.now()
timeNow = timeNow.strftime("%H-%M-%S")

# First function  - Used to load data in #
def companyDataframe(tickerPath):
    """
    tickerPath: path to file containing tickers in csv format
    returns ticker dataframe with sector, industry and sector industry values
    """

    company_info = pd.read_csv(tickerPath)

    sector_industry = []

    # Creating sector industry category in data #
    for i in range(len(company_info)):
        text = company_info['sector'].iloc[i] + "-" + company_info['industry'].iloc[i]
        sector_industry.append(text)

    # Create new sector industry column in original dataframe #
    company_info['sector_industry'] = sector_industry

    # Clean industry column to replace hyphen with colon #
    company_info['industry'] = company_info['industry'].str.replace("—",':')

    return company_info


def newsCompanyDataframe(company_info,index,startTime,endTime):
    """
    company_info: dataframe containing tickers, sector, industry and sector industry
    index: index to search on
    startTime: Start time of query
    endTime: End time of query

    """

    # Initialize dataframe template
    apisearch = APISearch()
    tickers = company_info.ticker

    df = GenerateDataset(index)

    if index == 'news':
        # Create dataframe #
        for ticker in tickers:
            query = (APISearch.search_news(apisearch,search_string="",timestamp_from=startTime,timestamp_to=endTime,tickers=[ticker]))
            res = es_client.search(index=index, body=query, size=10000)
            dfTicker = GenerateDataset.createDataStructure(df,res['hits']['hits'])


        # Clean dataframe #
        dfTicker = dfTicker.drop_duplicates(subset=['title'])
        dfTicker = dfTicker.reset_index(drop=True)
        dfTicker = dfTicker.explode('tickers')
        dfTicker = dfTicker.reset_index(drop=True)

    else:
        for ticker in tickers:
            query = (APISearch.search_tweets(apisearch, search_string="", timestamp_from=startTime, timestamp_to=endTime,
                                           tickers=[ticker]))
            res = es_client.search(index=index, body=query, size=10000)
            dfTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])

        # Clean dataframe #
        dfTicker = dfTicker.drop_duplicates(subset=['tweet'])
        dfTicker = dfTicker.reset_index(drop=True)
        dfTicker = dfTicker.explode('cashtags')
        dfTicker = dfTicker.reset_index(drop=True)

    # Append sectors,industry and sector industry columns to dataframe #
    sectors = {k: [] for k in range(len(dfTicker))}
    industry = {k: [] for k in range(len(dfTicker))}
    sectorIndustry = {k: [] for k in range(len(dfTicker))}

    for i in range(len(dfTicker)):

        try:
            sectors[i].append((company_info[company_info['ticker'] == dfTicker.cashtags[i]].sector.iloc[0]))
        except Exception as e:
            pass

        try:
            industry[i].append((company_info[company_info['ticker'] == dfTicker.cashtags[i]].industry.iloc[0]))
        except Exception as e:
            pass

        try:
            sectorIndustry[i].append((company_info[company_info['ticker'] == dfTicker.cashtags[i]].sector_industry.iloc[0]))
        except Exception as e:
            pass

    # Creating new columns in dataframe #
    dfTicker['sector'] = pd.Series(sectors)
    dfTicker['industry'] = pd.Series(industry)
    dfTicker['sector_industry'] = pd.Series(sectorIndustry)

    # convert sector, industry and sector industry values to tuples to be groupby'd #
    for i in range(len(dfTicker)):
        dfTicker.sector.iloc[i] = tuple(dfTicker.sector.iloc[i])
        dfTicker.industry.iloc[i] = tuple(dfTicker.industry.iloc[i])
        dfTicker.sector_industry.iloc[i] = tuple(dfTicker.sector_industry.iloc[i])

    return dfTicker

def generateSectorIndustryDataFrames(dfTicker,sentimentBound,dataframeType):

    """
    dfTicker: Filtered dataframe based on sentiment value bounds
    sentimentBound: Bound value for sentiment fitler
    dataframeType: 'sector', 'industry' or 'sector_industry' -> indicates what type of dataframe you want.

    """

    dfTicker = dfTicker[(dfTicker.sentiment_score > abs(sentimentBound)) | (dfTicker.sentiment_score < -1*abs(sentimentBound))]

    df = pd.DataFrame((dfTicker.groupby(dataframeType)['sentiment_score'].mean().sort_values())).reset_index()


    for i in range(len(df)):
        try:
            df[dataframeType].iloc[i] = df[dataframeType].iloc[i][0]
        except Exception as e:
            df[dataframeType].iloc[i] = "None"

    return df


def sectorIndustryTickerNews(dfTicker,sentimentBound,dataframeType,index,n,topBottom):

    """
    dfTicker: Filtered dataframe based on sentiment value bounds
    sentimentBound: Bound value for sentiment fitler (-1 to 1)
    dataframeType: 'sector', 'industry' or 'sector_industry' -> indicates what type of dataframe you want.
    index: index to search on
    n: top n values
    topBottom: top or bottom - top will return most positive sentiment sector,industry, sector industry values
    tickers = False, set to True if you want the tickers corresponding to top/least sentiment categories

    returns a dataframe of the top N tickers for the top N or Bottom N sentiment and shows the now-1d news for those tickers
    """
    labelList = []

    dfTicker = dfTicker[(dfTicker.sentiment_score > abs(sentimentBound)) | (dfTicker.sentiment_score < -1*abs(sentimentBound))]

    df = pd.DataFrame((dfTicker.groupby(dataframeType)['sentiment_score'].mean().sort_values())).reset_index()

    filter_ = (list((df.groupby(dataframeType)['sentiment_score'].mean().sort_values()).index[-1::-1]))

    if topBottom == 'top':
        filter_ = filter_[0:n]
    else:
        filter_ = filter_[-n:]

    # Initialize the search #
    apisearch = APISearch()

    df = GenerateDataset(index)

    if index == 'news':

        for i in range(len(filter_)):
            try:
                filterTickers = list(company_info[company_info[dataframeType] == filter_[i][0]].ticker[0:100].values)
            except Exception as e:
                continue

            for ticker in filterTickers:
                try:
                    query = (APISearch.search_news(apisearch, search_string="", timestamp_from='now-1d', timestamp_to='now',
                                               tickers=[ticker]))
                    res = es_client.search(index=index, body=query, size=10000)

                    for j in range(len(res['hits']['hits'])):
                         labelList.append(filter_[i][0])

                except Exception as e:
                    pass


                dfFilterTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])

        dfFilterTicker[dataframeType] = labelList

        dfFilterTicker = dfFilterTicker.drop_duplicates(subset=['title'])

        dfFilterTicker = dfFilterTicker.loc[:,['title', 'published_datetime', 'tickers',dataframeType]]

    else:

        for i in range(len(filter_)):
            try:
                filterTickers = list(company_info[company_info[dataframeType] == filter_[i][0]].ticker[0:100].values)
            except Exception as e:
                continue

            for ticker in filterTickers:
                try:
                    query = (
                        APISearch.search_tweets(apisearch, search_string="", timestamp_from='now-1d', timestamp_to='now',
                                              tickers=[ticker]))
                    res = es_client.search(index=index, body=query, size=10000)

                    for j in range(len(res['hits']['hits'])):
                        labelList.append(filter_[i][0])

                except Exception as e:
                    pass

                dfFilterTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])

        dfFilterTicker[dataframeType] = labelList

        dfFilterTicker = dfFilterTicker.drop_duplicates(subset=['tweet'])

        dfFilterTicker = dfFilterTicker.loc[:, ['tweet', 'timestamp', 'cashtags', dataframeType]]

    return dfFilterTicker

if __name__ == '__main__':
    # input parameters #
    tickerPath = r"C:\Users\mp094\Desktop\Work CPU Desktop\LJZP inv\Stock Company Data\apr2021_company_information.csv"
    company_info = companyDataframe(tickerPath)
    startTime = 'now-1d'
    endTime = 'now'
    index = 'tweets'
    path_to_save = rf"E:\Data\Keywords\sentimentTickers_{dateNow}_{timeNow}.html"
    labels = ['sector','industry','sector_industry']
    sentiment = 0.6
    n = 10
    topBottom = 'top'

    dfTicker = newsCompanyDataframe(company_info,index,startTime,endTime)

    text_file = open(path_to_save, "w")

    for label in labels:

        #sentimentDF = (generateSectorIndustryDataFrames(dfTicker, sentiment, label))
        sentimentDF = sectorIndustryTickerNews(dfTicker,sentiment,label,index,n,topBottom)
        sentimentDF.to_csv(rf"E:\Data\Keywords\sentimentTickers_{dateNow}_{timeNow}.csv")

        text_file.write(sentimentDF.to_html(classes='table table-striped'))

    text_file.close()