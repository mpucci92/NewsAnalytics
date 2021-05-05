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
from SearchAPI import NewsVolumeQuery
from CONFIG import configFile

CONFIG = configFile()
es_client = Elasticsearch([CONFIG['elasticsearchIP']],http_compress=True)


class NewsVolumeIndicator:

    def __init__(self, index, tickers):
        self.index = index.lower()
        self.tickers = tickers

        self.indicatorColumns = [
            'CompanyName',
            'Ticker',
            'MentionsCount'
        ]
        self.indicatorStructures = {
            'CompanyName': [],
            'Ticker': [],
            'MentionsCount': []
        }

        self.startEndDates = {
            'startDate': [],
            'endDate': []
        }

    def volumeIndicator(self, startDate, endDate, sortField, sortOrder, companyData=None, dataPath=None):

        """
        Base function to obtain how many mentions stock received on specified time basis

        """
        dataPath = r"C:\Users\mp094\Desktop\Company Tagger\NewsLab\clean\data\curated_company_names.csv"
        tickerData = pd.read_csv(dataPath)

        df = pd.DataFrame(columns=self.indicatorColumns)

        if (self.index.lower()) == 'news':

            if type(companyData) == list:
                companyData = self.tickers

                for ticker in list(set(companyData)):
                    query = NewsVolumeQuery.newsVolumeQuery(ticker, startDate, endDate, sortField, sortOrder)

                    try:
                        res = es_client.search(index=self.index, body=query, size=10000)
                        self.indicatorStructures['CompanyName'].append(
                            tickerData[tickerData['ticker'] == ticker]['name'].iloc[0])
                        self.indicatorStructures['Ticker'].append(ticker)
                        self.indicatorStructures['MentionsCount'].append(res['hits']['total']['value'])

                    except Exception as e:
                        pass

            else:
                companyData = pd.read_csv(dataPath)
                uniqueTickerList = (list(set(companyData['ticker'])))

                for ticker in uniqueTickerList:
                    query = NewsVolumeQuery.newsVolumeQuery(ticker, startDate, endDate, sortField, sortOrder)

                    try:
                        res = es_client.search(index=self.index, body=query, size=10000)
                        self.indicatorStructures['CompanyName'].append(
                            tickerData[tickerData['ticker'] == ticker]['name'].iloc[0])
                        self.indicatorStructures['Ticker'].append(ticker)
                        self.indicatorStructures['MentionsCount'].append(res['hits']['total']['value'])

                    except Exception as e:
                        pass

            for col in self.indicatorColumns:
                df[col] = self.indicatorStructures[col]


        elif self.index.lower() == 'tweets':

            if type(companyData) == list:
                companyData = self.tickers

                for ticker in list(set(companyData)):
                    query = NewsVolumeQuery.tweetVolumeQuery(ticker, startDate, endDate, sortField, sortOrder)

                    try:
                        res = es_client.search(index=self.index, body=query, size=10000)
                        self.indicatorStructures['CompanyName'].append(
                            tickerData[tickerData['ticker'] == ticker]['name'].iloc[0])
                        self.indicatorStructures['Ticker'].append(ticker)
                        self.indicatorStructures['MentionsCount'].append(res['hits']['total']['value'])

                    except Exception as e:
                        pass

            else:
                companyData = pd.read_csv(dataPath)
                uniqueTickerList = (list(set(companyData['ticker'])))

                for ticker in uniqueTickerList:
                    query = NewsVolumeQuery.newsVolumeQuery(ticker, startDate, endDate, sortField, sortOrder)

                    try:
                        res = es_client.search(index=self.index, body=query, size=10000)
                        self.indicatorStructures['CompanyName'].append(
                            tickerData[tickerData['ticker'] == ticker]['name'].iloc[0])
                        self.indicatorStructures['Ticker'].append(ticker)
                        self.indicatorStructures['MentionsCount'].append(res['hits']['total']['value'])

                    except Exception as e:
                        pass

            for col in self.indicatorColumns:
                df[col] = self.indicatorStructures[col]



        else:
            print("INCORRECT INDEX SYSTEM SHUT DOWN")
            sys.exit()

        # print("STARTTIME: " + startDate + "  ENDTIME: " + endDate)

        return df

    def newsVolumeIndicatorFixed(self, startTimeFrame, endTimeFrame=None):

        """
        1y INTERVAL 1 YEAR

        1M INTERVAL 1 MONTH

        1w INTERVAL 1 WEEK

        1d INTERVAL 1 DAY

        1h INTERVAL 1 HOUR

        1m INTERVAL 1 MINUTE

        1s INTERVAL 1 SECOND


        tickers: list of tickers

        startTimeFrame: second,minute,hour,day,week,month or Custom input

        index: news or tweets index

        custom: switch to indicate a custom timeframe is required

        """

        # Predefined times
        if startTimeFrame == '1 second':
            startTime = 'now-1s'
            endTime = 'now'

        elif startTimeFrame == '1 minute':
            startTime = 'now-1m'
            endTime = 'now'

        elif startTimeFrame == '1 hour':
            startTime = 'now-1h'
            endTime = 'now'

        elif startTimeFrame == '1 day':
            startTime = 'now-1d'
            endTime = 'now'

        elif startTimeFrame == '1 week':
            startTime = 'now-1w'
            endTime = 'now'

        elif startTimeFrame == '1 month':
            startTime = 'now-1M'
            endTime = 'now'

        elif startTimeFrame == '3 month':
            startTime = 'now-3M'
            endTime = 'now'

        elif startTimeFrame == '1 year':
            startTime = 'now-1y'
            endTime = 'now'

        else:
            startTime = startTimeFrame
            if endTimeFrame != None:
                endTime = endTimeFrame
            else:
                endTime = 'now'

        # Index selection
        if self.index == 'news':
            sortField = 'published_datetime'

        elif self.index == 'tweets':
            sortField = 'timestamp'

        dfFixed = self.volumeIndicator(startTime, endTime, sortField,
                                       'desc', companyData=self.tickers,
                                       dataPath=r"C:\Users\mp094\Desktop\Company Tagger\NewsLab\clean\data\curated_company_names.csv")

        timestampStart = []
        timestampEnd = []

        for i in range(len(dfFixed)):
            timestampStart.append(startTime)
            timestampEnd.append(endTime)

        dfFixed['StartTimestamp'] = timestampStart
        dfFixed['EndTimestamp'] = timestampEnd

        return dfFixed

    def volumeIndicatorTimeSeries(self, lookbackPeriod, timeframe, customDate=None, customInterval=None):

        """
        Method to generate a timeseries for news volume on a per stock basis

        lookbackPeriod = How many periods to go back into the past

        datapath = file path to curated names csv

        timeframe = preset timeframes or custom timeframes

        customDate = custom date to perform lookback period on

        customInterval = if not None, will be the interval used for timeseries

        """
        datapath = r"C:\Users\mp094\Desktop\Company Tagger\NewsLab\clean\data\curated_company_names.csv"

        listOfLists = [[] for i in range(len(self.tickers))]

        stockDict = {k: v for k, v in zip(self.tickers, listOfLists)}

        # logic for starting date for timeseries
        if customDate != None:
            date = pd.to_datetime(customDate)

        else:
            date = datetime.utcnow()

        for i in [i for i in [i for i in range(lookbackPeriod + 1)][::-1]][:-1]:

            formattedDateNowStart = date.strftime('%Y-%m-%dT%H:%M:%S')

            if timeframe == '1 minute':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - timedelta(minutes=i)

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - timedelta(minutes=i - 1)
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            elif timeframe == '5 minute':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (5 * timedelta(minutes=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (5 * timedelta(minutes=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            elif timeframe == '15 minute':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (15 * timedelta(minutes=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (15 * timedelta(minutes=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)


            elif timeframe == '1 hour':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(hours=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(hours=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)


            elif timeframe == '2 hour':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (2 * timedelta(hours=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (2 * timedelta(hours=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)


            elif timeframe == '4 hour':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (4 * timedelta(hours=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (4 * timedelta(hours=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            elif timeframe == '1 day':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(days=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(days=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            elif timeframe == '1 week':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (7 * timedelta(days=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (7 * timedelta(days=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            elif timeframe == 'custom':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (customInterval * timedelta(days=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (customInterval * timedelta(days=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                self.startEndDates['endDate'].append(endDate)

            if self.index == 'news':
                dfTimeSeries = self.volumeIndicator(startDate, endDate, 'published_datetime',
                                                    'desc', companyData=self.tickers,
                                                    dataPath=datapath)

            elif self.index == 'tweets':
                dfTimeSeries = self.volumeIndicator(startDate, endDate, 'timestamp',
                                                    'desc', companyData=self.tickers,
                                                    dataPath=datapath)

        for tickerName in (self.tickers):
            stockDict[tickerName] = (list(dfTimeSeries[dfTimeSeries['Ticker'] == tickerName]['MentionsCount']))

        return stockDict
