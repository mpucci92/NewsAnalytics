import pandas as pd
from NewsVolumeIndicator import NewsVolumeIndicator
from NewsStatistics import StatsDataStructure

class NewsVolumeStatistics():

    def zScore(self,value, mean, std):
        z = (value - mean) / std

        return round(z, 3)


    def percentile(self,value, list_):
        filteredList = [x for x in list_ if x < value]
        percentileValue = (len(filteredList) / len(list_)) * 100

        return round(percentileValue, 3)


    def newsVolumeStatistics(self,index, tickers):
        data = {new_list: [] for new_list in tickers}

        for ticker in tickers:

            # 1D Total Volume
            tickerList = []
            tickerList.append(ticker)

            try:
                dayTotal = NewsVolumeIndicator(index, tickerList)
                oneDay = round(dayTotal.newsVolumeIndicatorFixed('now-1d', endTimeFrame='now')['MentionsCount'].iloc[0], 3)
                data[ticker].append(oneDay)
            except Exception as e:
                data[ticker].append(None)

            # 5D Average Volume
            statistics = StatsDataStructure()
            try:
                fiveDayAverage = round(statistics.newsCalculations(index, tickerList, [5], ['1 day'])['mean'].iloc[0], 3)
                data[ticker].append(fiveDayAverage)
            except Exception as e:
                data[ticker].append(None)

            # 5D Ratio
            try:
                data[ticker].append(round(((oneDay / fiveDayAverage)), 3))
            except Exception as e:
                data[ticker].append(None)

            # 20D Average Volume
            statistics = StatsDataStructure()
            try:
                twentyDayAverage = round(statistics.newsCalculations(index, tickerList, [20], ['1 day'])['mean'].iloc[0], 3)
                data[ticker].append(twentyDayAverage)
            except Exception as e:
                data[ticker].append(None)

            # 20D Ratio
            try:
                data[ticker].append(round(((oneDay / twentyDayAverage)), 3))
            except Exception as e:
                data[ticker].append(None)

            # 20D Sigma
            statistics = StatsDataStructure()
            try:
                twentyDaySigma = round(
                    statistics.newsCalculations(index, tickerList, [20], ['1 day'])['standard deviation'].iloc[0], 3)
            except Exception as e:
                pass

            # 20D Zscore
            try:
                twentyDayZ = (self.zScore(oneDay, twentyDayAverage, twentyDaySigma))
                data[ticker].append(twentyDayZ)
            except Exception as e:
                data[ticker].append(None)

            # 20D Rank
            obj = NewsVolumeIndicator(index, tickerList)
            try:
                twentyDayPercentile = self.percentile(oneDay,
                                                 list((pd.Series(obj.volumeIndicatorTimeSeries(20, '1 day'))).iloc[0]))
                data[ticker].append(twentyDayPercentile)
            except Exception as e:
                data[ticker].append(None)

        #print(data)

        df = pd.DataFrame.from_dict(data, orient='index').reset_index()
        df.columns = ['ticker', '1D Volume', '5D Average', '5D Ratio', '20D Average', '20D Ratio', '20D Zscore', '20D Rank']

        return df

