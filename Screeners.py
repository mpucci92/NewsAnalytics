from bs4 import BeautifulSoup
import requests
from bs4 import BeautifulSoup
from lxml import etree
import requests
import pandas as pd

class Screener():

    """

    Screener class used to generate dataframe from prebuilt Yahoo Finance Screeners
    URL: Url to the screener of choice from Yahoo Finance

    Returns: post-processed dataframe of Yahoo Finance Screener data

    """


    def __init__(self,URL):
        self.URL = URL

        self.HEADERS = ({'User-Agent':
                             'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                             (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', \
                         'Accept-Language': 'en-US, en;q=0.5'})

    def generateDomain(self):
        webpage = requests.get(self.URL, headers=self.HEADERS)
        soup = BeautifulSoup(webpage.content, "html.parser")
        dom = etree.HTML(str(soup))

        return dom

    def buildDataStructure(self,dom):
        tickers = []
        stockNames = []
        closingPrice = []
        percentage = []
        volumes = []
        threeMonthVolume = []
        marketCap = []
        PERatio = []

        # Stock names
        for i in range(1, 101):
            try:
                stockNames.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[2]/text()')[0])
            except Exception as e:
                stockNames.append(None)

        # tickers
        for i in range(1, 101):
            try:
                tickers.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[1]/a')[0].text)
            except Exception as e:
                tickers.append(None)

        # closing price
        for i in range(1, 101):
            try:
                closingPrice.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[3]/span')[0].text)
            except Exception as e:
                closingPrice.append(None)

        # percentage
        for i in range(1, 101):
            try:
                percentage.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[5]/span')[0].text)
            except Exception as e:
                percentage.append(None)

        # volumes
        for i in range(1, 101):
            try:
                volumes.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[6]/span')[0].text)
            except Exception as e:
                volumes.append(None)

        # 3M Volume
        for i in range(1, 101):
            try:
                threeMonthVolume.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[7]/text()')[0])
            except Exception as e:
                threeMonthVolume.append(None)

        # Market Cap
        for i in range(1, 101):
            try:
                marketCap.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[8]/span')[0].text)
            except Exception as e:
                marketCap.append(None)

        # P/E RATIO
        for i in range(1, 101):
            try:
                PERatio.append(dom.xpath(f'//*[@id="scr-res-table"]/div[1]/table/tbody/tr[{i}]/td[9]/text()')[0])
            except Exception as e:
                PERatio.append(None)

        df = pd.DataFrame()
        df['tickers'] = tickers
        df['company'] = stockNames
        df['closing price'] = closingPrice
        df['percentage'] = percentage
        df['volume'] = volumes
        df['3M avg volume'] = threeMonthVolume
        df['market cap'] = marketCap
        df['PE ratio'] = PERatio

        df = df.dropna(axis=0, subset=['tickers', 'company'])
        df['PE ratio'] = df['PE ratio'].fillna("0")

        # text preprocessing 1
        for i in range(len(df)):
            df['volume'].iloc[i] = df.volume.iloc[i].replace(",", "")
            df['3M avg volume'].iloc[i] = df['3M avg volume'].iloc[i].replace(",", "")
            df['market cap'].iloc[i] = df['market cap'].iloc[i].replace(",", "")
            df['PE ratio'].iloc[i] = df['PE ratio'].iloc[i].replace(",", "")
            df.percentage.iloc[i] = float(df.percentage.iloc[i].replace("+", "").replace("%", ""))

        # text preprocessing 2
        df['volume'] = df['volume'].replace({'K': '*1e3', 'M': '*1e6', 'B': '*1e9','T': '*1e12'},
                                            regex=True).map(pd.eval).astype(float)
        df['3M avg volume'] = df['3M avg volume'].replace({'K': '*1e3', 'M': '*1e6', 'B': '*1e9','T': '*1e12'},
                                                          regex=True).map(pd.eval).astype(float)
        df['market cap'] = df['market cap'].replace({'K': '*1e3', 'M': '*1e6', 'B': '*1e9','T': '*1e12'},
                                                    regex=True).map(pd.eval).astype(float)

        # text preprocessing 3
        df['PE ratio'] = df['PE ratio'].astype(float)

        # column adjustments
        df.percentage = df.percentage.astype(float)
        df['volume:3m volume'] = df.volume / df['3M avg volume']

        return df




