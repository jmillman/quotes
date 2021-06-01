import pandas as pd
import glob
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
import datetime


driver = webdriver.Chrome("./chromedriver")
directory_daily_history = "./local_stock_history"
file_name_gap_up_summary = 'summary/gapped_up.csv'

file_name_finviz_summary = 'summary/finviz-{}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

def save_gap_up_data_to_summary_file():
  files = glob.glob("{}/*.csv".format(directory_daily_history))

  results =  pd.DataFrame()
  for i in range(len(files)):
    print(files[i])
    history = pd.read_csv(files[i])

    if history['low'].min() != 0:
      history['datetime'] = pd.to_datetime(history['datetime'])
      history['date_only'] = history['datetime'].dt.date
      history['close_yesterday'] = history.close.shift(1)
      history['diff_percent'] = (history['open'] - history['close_yesterday']) * 100 / history['close_yesterday']
      history['gap_up'] = (history['diff_percent'] > 30) & (history['volume'] > 1000000)
      gapped_up = history[history['gap_up'] == True]
      if len(gapped_up):
        results = results.append(gapped_up)

  results.to_csv(file_name_gap_up_summary, mode='a+', header=True, index=False)

def get_symbols_over_million_volume():
  files = glob.glob("{}/*.csv".format(directory_daily_history))

  results =  set()
  for i in range(len(files)):
    print(files[i])
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
  keys = [
    "Market Cap",
    "Short Float",
    "Shs Float",
    "Shs Outstand",
  ]
  for key in keys:
    results[key] = table.findChildren("td", string=key)[0].next_sibling.string
  return results



def save_active_stocks_finviz_to_file():
  symbols_with_volume = get_symbols_over_million_volume()
  for index, symbol in enumerate(symbols_with_volume):
    finviz = get_data_finviz(symbol)
    finviz_df = pd.DataFrame(finviz, index=[0])
    showHeaders = True if index == 0 else False
    finviz_df.to_csv(file_name_finviz_summary, mode='a+', header=showHeaders, index=False)


if __name__ == "__main__":
  # save_gap_up_data_to_summary_file()
  save_active_stocks_finviz_to_file()
