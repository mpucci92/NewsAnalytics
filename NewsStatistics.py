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
from NewsVolumeIndicator import NewsVolumeIndicator

class StatsDataStructure:


    dictStatsFixedStructures = {
        'ticker': []
    }

    def newsVelocityRateOfChange(self, index, tickers, numberMeasure, quantityMeasure):

        """
        index: index of elasticsearch to query
        tickers: list input of tickers to search

        numberMeasure: 10, 20 , 30 , 60 , 120
        quantityMeasure: minute, hour, days, week, months

        return: dictionary of all the statistical measures in stockStatsDict per stock mentioned in stock tickers list

        """

        # data path that points to where company list is saved - can be variable
        datapath = r"C:\Users\mp094\Desktop\Company Tagger\NewsLab\clean\data\curated_company_names.csv"

        statisticalObj = NewsVolumeIndicator(index, tickers)

        stockDict = statisticalObj.volumeIndicatorTimeSeries(numberMeasure, quantityMeasure)

        stockStatsDict = {}

        for ticker in statisticalObj.tickers:
            stockStatsDict.update({f"{ticker}": {'count': pd.Series(stockDict[f"{ticker}"]).describe()['count'],
                                                 'min': pd.Series(stockDict[f"{ticker}"]).describe()['min'],
                                                 'median': pd.Series(stockDict[f"{ticker}"]).describe()['50%'],
                                                 'mean': pd.Series(stockDict[f"{ticker}"]).describe()['mean'],
                                                 'max': pd.Series(stockDict[f"{ticker}"]).describe()['max'],
                                                 'standard deviation': pd.Series(stockDict[f"{ticker}"]).describe()[
                                                     'std'],
                                                 'sum': pd.Series(stockDict[f"{ticker}"]).sum()}})

        return stockStatsDict


    def newsCalculations(self, index, tickers, timeSeriesUnits, timeSeriesTimeFrame):

        """
        index: index on ElasticSearch to query data on
        tickers: list of tickers to aggregate total news on
        timeSeriesUnits: numerical quantity of time
        timeSeriesTimeFrame: timeframe that goes in relation to the timeSeriesUnits

        return: dataframe that contains all the statistics for each ticker
        """

        timestamps = []
        totalStatistics = []

        for time in timeSeriesUnits:
            for timeframe in timeSeriesTimeFrame:
                statisticsData = StatsDataStructure()
                totalStatistics.append(statisticsData.newsVelocityRateOfChange(index, tickers, time, timeframe))

        # individual statistics for each ticker
        tickerStatistics = []
        for i in range(len(totalStatistics)):
            for key in totalStatistics[i].values():
                tickerStatistics.append(list(key.values()))

        statsDF = pd.DataFrame(tickerStatistics, columns=['count',
                                                          'min',
                                                          'median',
                                                          'mean',
                                                          'max',
                                                          'standard deviation',
                                                          'sum'
                                                          ])

        # ticker list associated with the data
        listOfTickers = []
        for i in range(len(totalStatistics)):
            for ticker in tickers:
                listOfTickers.append(ticker)

        # Timestamp - unit of time
        for unit in timeSeriesUnits:
            for time in timeSeriesTimeFrame:
                for ticker in tickers:
                    timestamps.append(time)

        # Final creation of Dataframe to be returned
        statsDF['ticker'] = listOfTickers
        statsDF['timestamp'] = timestamps
        statsDF = statsDF[['ticker', 'count', 'min', 'median', 'mean', 'max', 'standard deviation', 'sum', 'timestamp']]

        return statsDF



    def timeSeriesCalcFilter(self,index,tickers,unitsTime,timeframe,filterMeasure,ticker=None):

        """
        index: index in elasticsearch to query data on
        ticker: Manual input for specific ticker output
        tickers: set of tickers to perform calculation filter on
        unitsTime: quantity of time
        timeframe: time measure (hour,day,week,etc)
        filterMeasure: statistical measure to filter against (mean,median,min,max,etc)

        return: dataframe containg filtered statistical measure applied to timeseries for given ticker
        """

        obj1 = StatsDataStructure()
        newsCalcDF = obj1.newsCalculations(index,tickers,unitsTime,timeframe)

        newsTimeSeriesObj = NewsVolumeIndicator(index,tickers)

        time = unitsTime[0]
        frame = timeframe[0]

        if ticker != None:
            dataframeData = (list(zip(newsTimeSeriesObj.volumeIndicatorTimeSeries(time,frame)[ticker],newsTimeSeriesObj.startEndDates['startDate'],newsTimeSeriesObj.startEndDates['endDate'])))

            filter_ = newsCalcDF[(newsCalcDF['ticker'] == ticker) & (newsCalcDF['timestamp'] == frame) & (newsCalcDF['count'] == time)][filterMeasure].iloc[0]

            df = pd.DataFrame(dataframeData,columns=['newsTotal','startDate','endDate'])

            df = df[df['newsTotal'] > filter_ ]

        else:
            for ticker in tickers:
                dataframeData = (list(zip(newsTimeSeriesObj.volumeIndicatorTimeSeries(time,frame)[ticker],newsTimeSeriesObj.startEndDates['startDate'],newsTimeSeriesObj.startEndDates['endDate'])))

                filter_ = newsCalcDF[(newsCalcDF['ticker'] == ticker) & (newsCalcDF['timestamp'] == frame) & (newsCalcDF['count'] == time)][filterMeasure].iloc[0]

                df = pd.DataFrame(dataframeData,columns=['newsTotal','startDate','endDate'])

                df = df[df['newsTotal'] > filter_ ]

                print(df)

        return df

    def ROC(self,timeseries):
        rocValues = []

        for i in range(len(timeseries)):
            if i != 0:
                try:
                    rateOfChange = round((timeseries[i] / timeseries[i - 1] - 1) * 100, 2)
                except Exception as e:
                    rateOfChange = None

                rocValues.append(rateOfChange)

            else:
                rocValues.append(None)

        return rocValues

    def tickerRateOfChange(self,index, tickers, lookbackPeriod, timeframe, customDate=None, customInterval=None):

        newsObj = NewsVolumeIndicator(index, tickers)

        if customDate != None:
            ROC_TS = newsObj.volumeIndicatorTimeSeries(lookbackPeriod, timeframe, customDate=customDate,
                                                       customInterval=customInterval)

        else:
            ROC_TS = newsObj.volumeIndicatorTimeSeries(lookbackPeriod, timeframe, customInterval=customInterval)

        tickerROCS = {}

        for ticker in tickers:
            if ticker in newsObj.indicatorStructures['Ticker']:
                tickerROCS[ticker] = self.ROC(ROC_TS[ticker])  # Return the dictionary which stores the data

        df = pd.DataFrame()
        df = pd.DataFrame(tickerROCS)

        df['StartTimestamp'] = newsObj.startEndDates['startDate']
        df['EndTimestamp'] = newsObj.startEndDates['endDate']

        return df