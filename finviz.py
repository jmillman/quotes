# -*- coding: utf-8 -*-
import pandas as pd
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os
import re


directory_daily_history = "./stock_history/2021/daily"
# file_name_finviz_summary = 'summary/finviz-{}.csv'.format(datetime.now().strftime("%Y-%m-%d"))



# CHROMEDRIVER = os.path.abspath("chromedriver.exe")
# BINARY_LOCATION = os.path.abspath(r"chromium\chrome.exe")

# chrome_options = webdriver.chrome.options.Options()
# chrome_options.add_argument("--no-sandbox")
# driver = webdriver.Chrome("./chromedriver", options=chrome_options)
#
# For some reason, yahoo loads much faster headless
# chrome_options2 = webdriver.chrome.options.Options()
# chrome_options2.add_argument("--headless")
# driver_yahoo = webdriver.Chrome("./chromedriver", options=chrome_options2)


def get_file(symbol, date):
    year, month, day = date.split('-')
    file_name_five_min = "./stock_history/{}/five_minute/{}/{}.csv".format(year, month, symbol)
    return file_name_five_min

def get_symbols_over_million_volume():
    files = glob.glob("{}/*.csv".format(directory_daily_history))

    results = set()
    for i in range(len(files)):
        # converter needed for symbol TRUE
        history = pd.read_csv(files[i], converters={"symbol": str})
        if history['volume'].max() > 1000000:
            results.add(history.iloc[0]['symbol'])
            if True in results:
                print(results)
    return results

def get_data_finviz(symbol):
    results = {}
    results["Symbol"] = symbol

    url = "https://finviz.com/quote.ashx?t=" + symbol
    driver.get(url)
    time.sleep(0)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"class": "snapshot-table2"})
    if table:
        keys = [
            "Market Cap",
            "Short Float",
            "Shs Float",
            "Shs Outstand",
        ]
        for key in keys:
            results[key] = table.findChildren("td", string=key)[0].next_sibling.string
    return results

def scrape_yahoo_stats(soup, results, search_item):
  key = search_item[0]
  search_text = search_item[1]
  span_with_text = soup.find_all("span", text=re.compile('.*' + search_text + '.*'))
  value = 'Not Found'
  title = ''
  if span_with_text:
    title = span_with_text[0].parent.parent.contents[0].get_text()
    value = span_with_text[0].parent.parent.contents[1].get_text()
  results[key] = {
    'title': title,
    'value': value,
  }
  return

def get_data_yahoo(symbol):
  url = 'https://finance.yahoo.com/quote/' + symbol + '/key-statistics'

  driver_yahoo.get(url)

  # this is just to ensure that the page is loaded
  time.sleep(0)

  html = driver_yahoo.page_source

  soup = BeautifulSoup(html, "html.parser")
  results = {}
  search_items = {}
  search_items['market_cap'] = 'Market Cap '
  search_items['shares_outstanding'] = 'Shares Outstanding'
  search_items['float'] = 'Float'
  search_items['held_by_insiders'] = 'Held by Insiders'
  search_items['held_by_institutions'] = 'Held by Institutions'
  search_items['shares_short'] = 'Shares Short'
  search_items['52_week_change'] = '52-Week Change'
  search_items['52_week_high'] = '52 Week High'
  search_items['52_week_low'] = '52 Week Low'
  search_items['50_day_moving_avg'] = '50-Day Moving Average'
  search_items['200_day_moving_avg'] = '200-Day Moving Average'
  search_items['avg_vol_3_mo'] = 'Avg Vol (3 month)'
  search_items['avg_vol_10_mo'] = 'Avg Vol (10 day)'
  search_items['shares_outstanding'] = 'Shares Outstanding'
  search_items['short_ratio'] = 'Short Ratio'
  search_items['short_percent_of_float'] = 'Short % of Float'
  search_items['short_percent_of_shares_outstanding'] = 'Short % of Shares Outstanding'
  for search_item in search_items.items():
    scrape_yahoo_stats(soup, results, search_item)

  return results

def save_active_stocks_finviz_to_file(file_name, update_only_missing_symbols=False):
    symbols = []
    if update_only_missing_symbols:
        gapped = pd.read_csv(file_name_gap)
        finviz = pd.read_csv(file_name_finviz)
        for i, row in gapped.iterrows():
            symbol = row['symbol']
            finviz_item = finviz[finviz['Symbol'] == symbol]
            if (len(finviz_item) == 0):
                gapped.loc[i, 'DATA_MISSING_FINVIZ'] = True
                symbols.append(symbol)
        symbols = sorted(set(symbols))
        print('Missing Symbols = {}'.format(symbols))
    else:
        symbols = sorted(set(get_symbols_over_million_volume()))

    for index, symbol in enumerate(symbols):
        finviz = get_data_finviz(symbol)
        df = None
        if('Shs Float' not in finviz or finviz['Shs Float'] == '-'):
            print('Yahoo {}'.format(symbol))
            yahoo_data = get_data_yahoo(symbol)
            yahoo = {}
            yahoo['Symbol'] = symbol
            yahoo['Market Cap'] = yahoo_data.get('market_cap').get('value')
            yahoo['Short Float'] = yahoo_data.get('short_percent_of_float').get('value')
            yahoo['Shs Float'] = yahoo_data.get('float').get('value')
            yahoo['Shs Outstand'] = yahoo_data.get('shares_outstanding').get('value')
            yahoo['source'] = 'YAHOO'
            df = pd.DataFrame(yahoo, index=[0])
            df.append(yahoo, ignore_index=True)
        else:
            print('FinViz {}'.format(symbol))
            finviz['source'] = 'FINVIZ'
            df = pd.DataFrame(finviz, index=[0])

        showHeaders = True if index == 0 else False
        df.to_csv(file_name, mode='a+', header=showHeaders, index=False)

def convert_billion_to_mill(str):
    if str == '-' or pd.isna(str):
        return 0
    elif 'k' in str:
        return float(str.replace("k", "")) / 1000
    elif 'B' in str:
        return float(str.replace("B", "")) * 1000
    else:
        return float(str.replace("M", ""))

if __name__ == "__main__":
    # Will save finviz data for any stock that had over a million in volume
    file_name = "./finviz/finviz-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))
    save_active_stocks_finviz_to_file(file_name=file_name)
    # this will just update the missing data, which is why the file_name is needed when date changes
    # save_active_stocks_finviz_to_file(update_only_missing_symbols=true, file_name=file_name)

    print('END')
