# FINAL #
from NewsTweetsVolume import *

def newsVolumeStatistics(dataframe, min=False,median=False,mean=False,max=False,std=False,cumsum=False,sum=False):
    if min == True:
        return dataframe.min()
    elif median == True:
        return dataframe.mean()
    elif mean == True:
        return dataframe.mean()
    elif max == True:
        return dataframe.max()
    elif std == True:
        return dataframe.std()
    elif cumsum == True:
        return dataframe.cumsum()
    elif sum == True:
        return dataframe.sum()


def ROC(timeseries):
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

if __name__ == '__main__':

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Parameters #
    tickers = ['GOOG', 'AAPL', 'FB', 'TSLA', 'APHA', 'TBP', 'SU', 'ENB', 'LSPD', 'BITF', 'WMT', 'NVAX', 'MMM']
    start_date = 'now-20d'
    end_date = 'now'
    index = 'news'
    time = '1 day'
    lookbackPeriod = 30


    df = createDataframeTS(index, tickers, lookbackPeriod, time)
    print(newsVolumeStatistics(df,sum=True).get(key='GOOG'))