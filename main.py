import pandas as pd
import glob
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta



# driver = webdriver.Chrome("./chromedriver")

directory_daily_history = "./local_stock_history"
file_name_gap_up_summary = 'summary/gapped_up.csv'
file_name_finviz_summary = 'summary/finviz-{}.csv'.format(datetime.now().strftime("%Y-%m-%d"))

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



def save_active_stocks_finviz_to_file():
  symbols_with_volume = sorted(get_symbols_over_million_volume())

  for index, symbol in enumerate(symbols_with_volume):
    finviz = get_data_finviz(symbol)
    finviz_df = pd.DataFrame(finviz, index=[0])
    showHeaders = True if index == 0 else False
    finviz_df.to_csv(file_name_finviz_summary, mode='a+', header=showHeaders, index=False)


def get_stock_by_date(symbol, date):
  file_name_daily = "./stock_history/{}.csv".format(symbol)
  daily_quotes = pd.read_csv(file_name_daily)
  daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime'])
  daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_localize('UTC')
  daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_convert('US/Eastern')
  daily_quotes['date_only'] = daily_quotes['datetime'].dt.date
  day_quote = daily_quotes[daily_quotes['date_only'] == datetime.strptime(date, '%Y-%m-%d').date()]
  return day_quote

def get_stats(symbol, date, start_time, end_time):
  file_name_five_min = "./stock_history_minute_five_extended_2021_05_28_to_2020_09_14/{}.csv".format(symbol)
  daily_quotes = pd.read_csv(file_name_five_min)
  # get the time in EST

  daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime'])
  daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_localize('UTC')
  daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_convert('US/Eastern')


  # daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime']) - timedelta(hours=4)
  # daily_quotes['date_only'] = daily_quotes['datetime'].dt.date

  start = '{} {}'.format(date, start_time)
  end = '{} {}'.format(date, end_time)

  day = daily_quotes[(daily_quotes['datetime'] >= start) & (daily_quotes['datetime'] <= end)]
  min_value = day['low'].min()
  max_value = day['high'].max()
  stats = {
    'min_value': min_value,
    'max_value': day['high'].max(),
    'min_time': day[day['low'] == min_value].iloc[0]['datetime'],
    'max_time': day[day['high'] == max_value].iloc[0]['datetime'],
  }
  return stats


if __name__ == "__main__":
  # save_gap_up_data_to_summary_file()
  # save_active_stocks_finviz_to_file()

  # quote = get_stock_by_date('TSLA', '2021-04-13')
  # all_pre_market = get_stats('TSLA', '2021-03-15', '04:00:00', '09:30:00')
  # pre_market_630 = get_stats('TSLA', '2021-03-15', '06:30:00', '09:30:00')
  # pre_market_630 = get_stats('TSLA', '2021-03-12', '06:30:00', '06:35:00')
  # open_market = get_stats('TSLA', '2021-03-15', '09:30:00', '10:30:00')
  # stats = get_stats('ASTC', '2021-04-07', '09:30:00', '16:00:00')


  # quote = get_stock_by_date('ASTC', '2021-03-12')
  # stats = get_stats('ASTC', '2021-03-12', '09:30:00', '16:00:00')
  # stats2 = get_stats('ASTC', '2021-03-15', '09:30:00', '16:00:00')
  #
  # print(stats)

  pre_market_730 = get_stats('TSLA', '2021-03-12', '07:30:00', '07:35:00')
  pre_market_830 = get_stats('TSLA', '2021-03-12', '08:30:00', '08:35:00')
  if(pre_market_730['min_value'] == 677.41):
    print('pre-daylight savings working')
  if(pre_market_830['min_value'] == 677.41):
    print('pre-daylight savings NOT working')

  pre_market_730_POST = get_stats('TSLA', '2021-03-15', '07:30:00', '07:35:00')
  pre_market_830_POST = get_stats('TSLA', '2021-03-15', '08:30:00', '08:35:00')
  if(pre_market_730_POST['min_value'] == 694.0):
    print('post-daylight savings working')
  else:
    print('post-daylight savings NOT working')

  print('END')
