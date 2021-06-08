# FINAL #

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
dt = datetime.now()
localTime = dt.date()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


indicatorStructures = {
            'CompanyName': {},
            'Ticker': {},
            'MentionsCount': {}
        }

startEndDates = {
            'startDate': [],
            'endDate': []
        }


def volumeIndicatorFixed(index,start_date, end_date, tickers):
    """
    Method used to populate indicatorStructures and startEndDates for given set of tickers for Fixed Series.

    index: index to query on (news or tweets) - Mandatory
    start_date: Custom or Preset and used as the starting date
    end_date: Custom or Preset and used as the end date
    tickers: Tickers to include in the timeseries - Mandatory

    """

    aggregation = NewsVolumeQuery()

    for ticker in tickers:
        tickerList = []
        countList = []

        query = aggregation.newsAggQuery(index,ticker, start_date,end_date)

        try:
            res = es_client.search(index=index, body=query, size=1)

            tickerList.append(ticker)

            indicatorStructures['Ticker'][ticker] = tickerList

            countList.append(res['aggregations']['ticker_counts']['buckets'][0]['doc_count'])
            indicatorStructures['MentionsCount'][ticker] = countList

        except Exception as e:
            pass



def volumeIndicatorTimeSeries(index,lookbackPeriod,tickers,timeframe, customDate=None, customInterval=None):
        """
        Method used to populate indicatorStructures and startEndDates for given set of tickers for a Time Series.

        index: index to query on (news or tweets) - Mandatory
        lookbackPeriod: periods to lookback - Mandatory
        tickers: Tickers to include in the timeseries - Mandatory
        timeframe: Preset timeframes or set to custom for custom date - Mandatory
        customDate: Optional - Use only if timeframe is set to custom
        customInterval: factor * hours, its factor to use to multiply by hours

        """

        # logic for starting date for timeseries
        if customDate != None:
            date = pd.to_datetime(customDate)

        else:
            date = datetime.utcnow()

        for ticker in tickers:
            tickerList = []
            countList = []

            for i in [i for i in [i for i in range(lookbackPeriod + 1)][::-1]][:-1]:

                formattedDateNowStart = date.strftime('%Y-%m-%dT%H:%M:%S')

                if timeframe == '1 minute':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - timedelta(minutes=i)

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - timedelta(minutes=i - 1)
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)

                elif timeframe == '5 minute':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (5 * timedelta(minutes=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (5 * timedelta(minutes=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)

                elif timeframe == '15 minute':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (15 * timedelta(minutes=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (15 * timedelta(minutes=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)


                elif timeframe == '1 hour':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(hours=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(hours=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)


                elif timeframe == '2 hour':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (2 * timedelta(hours=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (2 * timedelta(hours=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)


                elif timeframe == '4 hour':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (4 * timedelta(hours=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (4 * timedelta(hours=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)

                elif timeframe == '1 day':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(days=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(days=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)

                elif timeframe == '1 week':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (7 * timedelta(days=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (7 * timedelta(days=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)

                elif timeframe == 'custom':

                    dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (customInterval * timedelta(hours=i))

                    startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['startDate'].append(startDate)

                    formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                    dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (customInterval * timedelta(hours=i - 1))
                    endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                    startEndDates['endDate'].append(endDate)



                aggregation = NewsVolumeQuery()
                query = aggregation.newsAggQuery(index,ticker, startDate, endDate)

                try:
                    res = es_client.search(index=index, body=query, size=1)
                    tickerList.append(ticker)
                    indicatorStructures['Ticker'][ticker] = tickerList

                    countList.append(res['aggregations']['ticker_counts']['buckets'][0]['doc_count'])
                    indicatorStructures['MentionsCount'][ticker] = countList

                except Exception as e:
                    countList.append(None)


def createDataframeTS(index,tickers,lookbackPeriod,time,custom=False,customDate=None,customInterval=None):
        """
        Method used to generate Dataframe for given set of tickers as Time Series.

        index: index to query on (news or tweets) - Mandatory
        tickers: Tickers to include in the timeseries - Mandatory
        lookbackPeriod: periods to lookback - Mandatory
        time: Preset timeframes or set to custom for custom date - Mandatory
        custom: Boolean, set to True if the date provided is a custom date
        customDate: Optional - Use only if timeframe is set to custom
        customInterval: factor * hours, its factor to use to multiply by hours

        """

        df = pd.DataFrame()

        if custom == False:
            volumeIndicatorTimeSeries(index, lookbackPeriod, tickers, time)

            df['Start Date'] = startEndDates['startDate'][0:lookbackPeriod]

            for index, ticker in enumerate(tickers):
                keys = list((indicatorStructures['MentionsCount'].keys()))
                df[keys[index]] = indicatorStructures['MentionsCount'][ticker]

            for col in df.columns:
                df[col] = df[col].fillna(0)

            for col in df.columns:

                if df[col].dtype != 'object':
                    df[col] = df[col].astype(int)

        else:
            volumeIndicatorTimeSeries(index, lookbackPeriod, tickers, 'custom', customDate = customDate, customInterval = customInterval)

            df['Start Date'] = startEndDates['startDate'][0:lookbackPeriod]

            for index, ticker in enumerate(tickers):
                keys = list((indicatorStructures['MentionsCount'].keys()))
                df[keys[index]] = indicatorStructures['MentionsCount'][ticker]

            for col in df.columns:
                df[col] = df[col].fillna(0)

            for col in df.columns:

                if df[col].dtype != 'object':
                    df[col] = df[col].astype(int)

        return df


def createDataframeFixed(index, tickers,start_date,end_date):
    """
      Method used to generate Dataframe for given set of tickers as Fixed Series.

      index: index to query on (news or tweets) - Mandatory
      tickers: Tickers to include in the timeseries - Mandatory
      start_date: Custom or Preset and used as the starting date
      end_date: Custom or Preset and used as the end date

      """

    df = pd.DataFrame()
    df['Start Date'] = [start_date]

    volumeIndicatorFixed(index,start_date, end_date, tickers)

    for index,ticker in enumerate(tickers):
        keys = list((indicatorStructures['MentionsCount'].keys()))
        df[keys[index]] = (indicatorStructures['MentionsCount'][ticker])

    for col in df.columns:
        df[col] = df[col].fillna(0)

    for col in df.columns:

        if df[col].dtype != 'object':
            df[col] = df[col].astype(int)

    return df

def volumeIndicatorTimeSeriesAll(index, lookbackPeriod, timeframe, customDate=None, customInterval=None):
        """
            Method used to generate zipped list of timestamp,ticker,news count for all tickers as Time Series.

            index: index to query on (news or tweets) - Mandatory
            lookbackPeriod: periods to lookback - Mandatory
            timeframe: Preset timeframes or set to custom for custom date - Mandatory
            customDate: Optional - Use only if timeframe is set to custom
            customInterval: factor * hours, its factor to use to multiply by hours

            Returns a zipped list which can be used to generate dataframe that contains entire set of data
            USE ONLY if you want to collect ALL tickers for a given period - use function above for subset of tickers.

            """

        tickerList = []
        countList = []
        timestamp = []

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
                startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - timedelta(minutes=i - 1)
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['endDate'].append(endDate)

            elif timeframe == '1 hour':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(hours=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(hours=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['endDate'].append(endDate)

            elif timeframe == '1 day':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (timedelta(days=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (timedelta(days=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['endDate'].append(endDate)

            elif timeframe == '1 week':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (7 * timedelta(days=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (7 * timedelta(days=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['endDate'].append(endDate)

            elif timeframe == 'custom':

                dayBeforeStart = pd.to_datetime(formattedDateNowStart) - (customInterval * timedelta(hours=i))

                startDate = dayBeforeStart.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['startDate'].append(startDate)

                formattedDateNowEnd = date.strftime('%Y-%m-%dT%H:%M:%S')
                dayBeforeEnd = pd.to_datetime(formattedDateNowEnd) - (customInterval * timedelta(hours=i - 1))
                endDate = dayBeforeEnd.strftime('%Y-%m-%dT%H:%M:%S')
                startEndDates['endDate'].append(endDate)

            aggregation = NewsVolumeQuery()
            query = aggregation.allTickersQuery(index, startDate, endDate)

            try:
                res = es_client.search(index=index, body=query, size=1)

                for j in range(len(res['aggregations']['ticker_counts']['buckets'])):
                    timestamp.append(startDate)
                    ticker = res['aggregations']['ticker_counts']['buckets'][j]['key']
                    tickerList.append(ticker)

                    countList.append(res['aggregations']['ticker_counts']['buckets'][j]['doc_count'])

            except Exception as e:
                countList.append(None)

        return list(zip(timestamp, tickerList, countList))


if __name__ == '__main__':

    # Defining Tickers #
    #tickers = ['GOOG', 'AAPL', 'FB','TSLA','APHA','TBP','SU','ENB','LSPD','BITF','WMT','NVAX','MMM']
    #path = r"C:\Users\mp094\Desktop\Work CPU Desktop\LJZP inv\Stock Company Data\apr2021_company_information.csv"
    #dfTickers = pd.read_csv(path)
    #tickers = list(dfTickers.ticker)

    # Input parameters #
    start_date = 'now-20d'
    end_date = 'now'
    index = 'news'
    time = '1 day'
    lookbackPeriod = 9600
    customDate = '2021-05-23T00:00:00'
    customInterval = 1

    # Comment one or other for TS or FIXED #
    # Generate Fixed on screen time series for user chosen tickers
    #print(createDataframeTS(index, tickers, lookbackPeriod, time)) # Time Series
    #print(createDataframeFixed(index,tickers,start_date,end_date)) # Fixed

    # Generate timeseries or fixed for a custom date #
    #print(createDataframeTS(index, tickers, lookbackPeriod, 'custom', customDate = '2021-05-23T00:00:00', customInterval = 10)) # Time Series

    # USE to generate timeseries for all tickers
    #df = pd.DataFrame(
    #    volumeIndicatorTimeSeries(index, 9650, 'custom', customDate='2021-05-27T00:00:00', customInterval=1),
    #    columns=['timestamp', 'ticker', 'count'])