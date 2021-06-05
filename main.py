import pandas as pd
import glob
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta



# driver = webdriver.Chrome("./chromedriver")

directory_daily_history = "./stock_history"
file_name_gap = "./summary/gapped_up.csv"
file_name_finviz_summary = 'summary/finviz-{}.csv'.format(datetime.now().strftime("%Y-%m-%d"))

def save_gap_up_data_to_summary_file():
  files = sorted(glob.glob("{}/*.csv".format(directory_daily_history)))

  results =  pd.DataFrame()
  for i in range(len(files)):
    history = pd.read_csv(files[i])
    history['datetime'] = pd.to_datetime(history['datetime'])
    history['date_only'] = history['datetime'].dt.date

    # this is the earliest I have 5 min data
    start = datetime(2020, 9, 14).date()
    history = history[history['date_only'] >= start]

    if history['low'].min() != 0:
      history['close_yesterday'] = history.close.shift(1)
      history['diff_percent'] = (history['open'] - history['close_yesterday']) * 100 / history['close_yesterday']
      history['gap_up'] = (history['diff_percent'] > 30) & (history['volume'] > 1000000)
      gapped_up = history[history['gap_up'] == True]
      if len(gapped_up):
        results = results.append(gapped_up)

  results.to_csv(file_name_gap, mode='a+', header=True, index=False)

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
  if date > '2021-05-28':
    file_name_five_min = "./stock_history_minute_five_extended_2021_06_01_to_2021_06_04/{}.csv".format(symbol)
  else:
    file_name_five_min = "./stock_history_minute_five_extended_2021_05_28_to_2020_09_14/{}.csv".format(symbol)

  low = ''
  high = ''
  low_time = ''
  high_time = ''
  try:
    daily_quotes = pd.read_csv(file_name_five_min)
    # get the time in EST

    daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime'])
    daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_localize('UTC')
    daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_convert('US/Eastern')
    daily_quotes['time_only'] = daily_quotes['datetime'].dt.time

    # daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime']) - timedelta(hours=4)
    # daily_quotes['date_only'] = daily_quotes['datetime'].dt.date

    start = '{} {}'.format(date, start_time)
    end = '{} {}'.format(date, end_time)

    day = daily_quotes[(daily_quotes['datetime'] >= start) & (daily_quotes['datetime'] <= end)]
    low = day['low'].min()
    high = day['high'].max()

    if not pd.isna(low):
      low_time = day[day['low'] == low].iloc[0]['time_only']
      high_time = day[day['high'] == high].iloc[0]['time_only']
  except:
    print('{} file not found: {}  {}'.format(file_name_five_min, symbol, date))


  stats = {
    'low': low,
    'high': high,
    'low_time': low_time,
    'high_time': high_time,
  }
  return stats

def add_high_low_to_gap_up():
  gapped = pd.read_csv(file_name_gap)
  for i, row in gapped.iterrows():
    symbol = row['symbol']
    pre_market = get_stats(symbol, row['date_only'], '06:30:00', '09:30:00')
    morning = get_stats(symbol, row['date_only'], '09:30:00', '10:30:00')
    market = get_stats(symbol, row['date_only'], '09:30:00', '16:00:00')
    gapped.loc[i, 'pre_high'] = pre_market['high']
    gapped.loc[i, 'pre_low'] = pre_market['low']
    gapped.loc[i, 'pre_high_time'] = pre_market['high_time']
    gapped.loc[i, 'pre_low_time'] = pre_market['low_time']

    gapped.loc[i, 'morning_high'] = morning['high']
    gapped.loc[i, 'morning_low'] = morning['low']
    gapped.loc[i, 'morning_high_time'] = morning['high_time']
    gapped.loc[i, 'morning_low_time'] = morning['low_time']

    gapped.loc[i, 'market_high'] = market['high']
    gapped.loc[i, 'market_low'] = market['low']
    gapped.loc[i, 'market_high_time'] = market['high_time']
    gapped.loc[i, 'market_low_time'] = market['low_time']
  gapped.to_csv(file_name_gap, index=False)

def convert_billion_to_mill(str):
  if str == '-' or pd.isna(str):
    return 0
  elif 'B' in str:
    return float(str.replace("B", "")) * 1000
  else:
    return float(str.replace("M", ""))

def add_finviz_to_gap_up():
  file_name_finviz = "./summary/finviz-2021-05-31.csv"
  gapped = pd.read_csv(file_name_gap)
  finviz = pd.read_csv(file_name_finviz)
  for i, row in gapped.iterrows():
    symbol = row['symbol']
    finviz_item = finviz[finviz['Symbol'] == symbol]
    if(len(finviz_item)):
      gapped.loc[i, 'float'] = convert_billion_to_mill(finviz_item.iloc[0]['Shs Float'])
      shares = convert_billion_to_mill(finviz_item.iloc[0]['Shs Outstand'])
      gapped.loc[i, 'shares'] = shares
      gapped.loc[i, 'market_cap'] = "{:.2f}".format(shares * row['close_yesterday'])

  gapped.to_csv(file_name_gap, index=False)




if __name__ == "__main__":

  # quote = get_stock_by_date('ASTC', '2021-03-12')
  # stats = get_stats('ASTC', '2021-03-12', '09:30:00', '16:00:00')

  # Will save finviz data for any stock that had over a million in volume
  # save_active_stocks_finviz_to_file()

  # find gap up instances, save to file
  save_gap_up_data_to_summary_file()
  # go through instance and find the high low stats
  add_high_low_to_gap_up()
  #add the finvis info to the gap up
  add_finviz_to_gap_up()

  print('END')
