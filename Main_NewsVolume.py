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

es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)
index = 'news'

if __name__ == '__main__':

    screeners = [
        "most_actives",
        "day_losers",
        "day_gainers"
    ]

    for i in range(len(screeners)):
        tickersWithData = []

        screenerValue = screeners[i]

        # write html to file
        text_file = open(rf"E:\Data\HTML Reports\{screenerValue}_{formattedDateNow}.html", "w")

        URL = f"https://finance.yahoo.com/screener/predefined/{screenerValue}?count=100&offset=0"

        screener = Screener(URL)
        domain = Screener.generateDomain(screener)
        df = Screener.buildDataStructure(screener,domain)

        nVS = NewsVolumeStatistics()
        #print(NewsVolumeStatistics.newsVolumeStatistics(nVS,'news',list(df.tickers)))


        text_file.write(("REPORT:  %s" % (screenerValue)))
        text_file.write("*****************************************************************************************************")


        for ticker in list(df.tickers):
            apisearch = APISearch()
            query = (APISearch.search_news(apisearch,search_string="",timestamp_from='now-5d',timestamp_to='now',tickers=[ticker]))
            res = es_client.search(index=index, body=query, size=10000)
            df = GenerateDataset(index)

            dfTicker = GenerateDataset.createDataStructure(df, res['hits']['hits'])
            dfTicker = (dfTicker.drop_duplicates(subset=['title']))
            dfTicker = dfTicker.loc[:, ['published_datetime','title']]

            if len(dfTicker != 0):
                text_file.write("*****************************************************************************************************")
                text_file.write("")
                text_file.write("")
                text_file.write("")

                text_file.write("Ticker:         %s" % (ticker))
                #print("Ticker:         %s" % (ticker))
                try:
                    text_file.write("<br>")
                    text_file.write(dfTicker.to_html())
                except Exception as e:
                    pass
                #print(dfTicker)

            tickersWithData.append(ticker)


        if screenerValue != 'most_actives':
            text_file.write("*****************************************************************************************************")
            text_file.write("*****************************************************************************************************")
            text_file.write("*****************************************************************************************************")
            text_file.write("")
            text_file.write("---------------------NEWS-TWEETS VOLUME SUMMARY ------------------------")
            text_file.write("")
            text_file.write(" News Index --- Voluminous Companies")
            text_file.write("<br>")
            text_file.write("<br>")

            dfNewsVolumeStats = NewsVolumeStatistics.newsVolumeStatistics(nVS,'news',tickersWithData)
            dfFilteredNewsVolume = dfNewsVolumeStats[(dfNewsVolumeStats['5D Ratio'] > 2.0)
                                                     & (dfNewsVolumeStats['20D Ratio'] > 2.0)
                                                     & (dfNewsVolumeStats['20D Rank'] > 80)
                                                     & (dfNewsVolumeStats['20D Zscore'] > 1.5)]

            text_file.write(dfNewsVolumeStats.to_html())
            text_file.write("<br>")
            text_file.write("<br>")
            text_file.write("")
            text_file.write("*****************************************************************************************************")
            text_file.write("*****************************************************************************************************")
            text_file.write("*****************************************************************************************************")
            text_file.write("")
            text_file.write("Tweets Index --- Voluminous Companies")
            text_file.write("<br>")
            text_file.write("<br>")

            dfTweetsVolumeStats = NewsVolumeStatistics.newsVolumeStatistics(nVS, 'tweets', tickersWithData)
            dfFilteredNewsVolume = dfNewsVolumeStats[(dfNewsVolumeStats['5D Ratio'] > 2.0)
                                                     & (dfNewsVolumeStats['20D Ratio'] > 2.0)
                                                     & (dfNewsVolumeStats['20D Rank'] > 75)
                                                     & (dfNewsVolumeStats['20D Zscore'] > 1.5)]

            text_file.write(dfTweetsVolumeStats.to_html())

        text_file.write("<br>")
        text_file.write("<br>")
        text_file.write("News Index --- Rate of Change (DoD)")
        text_file.write("<br>")
        stats = StatsDataStructure()
        text_file.write(StatsDataStructure.tickerRateOfChange(stats, 'news', tickersWithData, 5, "1 day", customDate=None,
                                              customInterval=None).to_html())

        text_file.write("Tweets Index --- Rate of Change (DoD)")
        text_file.write("<br>")
        text_file.write(StatsDataStructure.tickerRateOfChange(stats, 'tweets', tickersWithData, 5, "1 day", customDate=None,
                                              customInterval=None).to_html())

        text_file.close()

        print("DONE")
        print(screenerValue)










