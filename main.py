import pandas as pd
import glob
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta



# driver = webdriver.Chrome("./chromedriver")

directory_daily_history = "./stock_history"
file_name_gap = "./summary/gapped_up-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))
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
    file_name_five_min = "./stock_history_five_minute_extended_2021_06_01_to_2021_CURRENT/{}.csv".format(symbol)
  else:
    file_name_five_min = "./stock_history_five_minute_extended_2020_09_14_to_2021_05_28/{}.csv".format(symbol)

  low = ''
  high = ''
  low_time = ''
  high_time = ''
  close = ''
  close_time = ''
  open = ''
  open_time = ''
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
    close = day['close'].iloc[-1]
    close_time = day['time_only'].iloc[-1]
    open = day['open'].iloc[1]
    open_time = day['time_only'].iloc[1]

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
    'close': close,
    'close_time': close_time,
    'open': open,
    'open_time': open_time
  }
  return stats

def add_columns(gapped, i, title, stats_obj):
  gapped.loc[i, '{}_open'.format(title)] = stats_obj['open']
  # gapped.loc[i, '{}_open_time'.format(title)] = stats_obj['open_time']
  gapped.loc[i, '{}_high'.format(title)] = stats_obj['high']
  # gapped.loc[i, '{}_high_time'.format(title)] = stats_obj['high_time']
  gapped.loc[i, '{}_low'.format(title)] = stats_obj['low']
  # gapped.loc[i, '{}_low_time'.format(title)] = stats_obj['low_time']
  gapped.loc[i, '{}_close'.format(title)] = stats_obj['close']
  # gapped.loc[i, '{}_close_time'.format(title)] = stats_obj['close_time']


def get_summary_data(gapped, i, date_only, title, symbol, start_time, end_time):
  summary = get_stats(symbol, date_only, start_time, end_time)
  add_columns(gapped, i, title, summary)


def add_booleans_to_gap_up():
  gapped = pd.read_csv(file_name_gap)
  gapped['pre_high_gte_daily_high'] = gapped['pre_high'] >= gapped['daily_high']
  gapped['second_fifteen_high10P_gte_first_fifteen_close'] = gapped['second_fifteen_high'] * 1.1 >= gapped['first_fifteen_close']
  gapped['second_thirty_high10P_gte_first_thirty'] = gapped['second_thirty_high'] * 1.1 >= gapped['first_thirty_close']
  gapped['daily_high_gt_pre_high10P'] = gapped['daily_high'] > gapped['pre_high'] * 1.1
  gapped.to_csv(file_name_gap, index=False)

def get_current_and_cum_volume(df, time_stamp):
  current_vol = df[(df['datetime'] == time_stamp)]["volume"].sum()
  total_vol = df[(df['datetime'] <= time_stamp)]["volume"].sum()
  return current_vol, total_vol

def add_volume_to_gap_up():
  gapped = pd.read_csv(file_name_gap)
  for i, row in gapped.iterrows():
    symbol = row["symbol"]
    date = row["date_only"]
    if date > '2021-05-28':
      file_name_five_min = "./stock_history_five_minute_extended_2021_06_01_to_2021_CURRENT/{}.csv".format(symbol)
    else:
      file_name_five_min = "./stock_history_five_minute_extended_2020_09_14_to_2021_05_28/{}.csv".format(symbol)

    try:
      daily_quotes = pd.read_csv(file_name_five_min)



      daily_quotes['date_only'] = pd.to_datetime(daily_quotes['datetime']).dt.date

      start = '{} {}'.format(date, "00:00:00")
      end = '{} {}'.format(date, "24:00:00")
      daily_quotes = daily_quotes[(daily_quotes['datetime'] >= start) & (daily_quotes['datetime'] <= end)]

      if row["float"] != 0:
        for five_min_index in range(37):
          time_stamp = datetime.strptime('{} 09:30:00'.format(date), '%Y-%m-%d %H:%M:%S') + (timedelta(minutes=5) * five_min_index)
          time_string = time_stamp.strftime('%Y-%m-%d %H:%M:%S')
          current_vol, total_vol = get_current_and_cum_volume(daily_quotes, time_string)
          gapped.loc[i, "f_{}_v_c".format(five_min_index)] = current_vol
          gapped.loc[i, "f_{}_v_t".format(five_min_index)] = total_vol
          gapped.loc[i, "f_{}_fr_c".format(five_min_index)] = current_vol / (row["float"] * 1000000)
          gapped.loc[i, "f_{}_fr_t".format(five_min_index)] = total_vol / (row["float"] * 1000000)

      else:
        print('{} has 0 float'.format(symbol))
    except:
      print('{} file not found: {}  {}'.format(file_name_five_min, symbol, date))

  gapped.to_csv(file_name_gap, index=False)


def add_high_low_to_gap_up():
  gapped = pd.read_csv(file_name_gap)
  for i, row in gapped.iterrows():
    symbol = row['symbol']
    date_only = row['date_only']

    get_summary_data(gapped, i, date_only, 'pre', symbol, '07:30:00', '09:30:00')
    get_summary_data(gapped, i, date_only, 'daily', symbol, '09:30:00', '16:00:00')
    get_summary_data(gapped, i, date_only, 'first_fifteen', symbol, '09:30:00', '9:45:00')
    get_summary_data(gapped, i, date_only, 'first_thirty', symbol, '09:30:00', '10:00:00')
    get_summary_data(gapped, i, date_only, 'first_hour', symbol, '09:30:00', '10:30:00')
    get_summary_data(gapped, i, date_only, 'ten_thirty_to_close', symbol, '10:30:00', '16:00:00')

    get_summary_data(gapped, i, date_only, 'second_fifteen', symbol, '09:45:00', '10:00:00')
    get_summary_data(gapped, i, date_only, 'second_thirty', symbol, '10:00:00', '10:30:00')
    get_summary_data(gapped, i, date_only, 'second_hour', symbol, '10:30:00', '11:30:00')
    get_summary_data(gapped, i, date_only, 'eleven_thirty_to_close', symbol, '11:30:00', '16:00:00')

    get_summary_data(gapped, i, date_only, 'third_fifteen', symbol, '10:00:00', '10:15:00')
    get_summary_data(gapped, i, date_only, 'third_thirty', symbol, '10:30:00', '11:00:00')
    get_summary_data(gapped, i, date_only, 'third_hour', symbol, '11:30:00', '12:30:00')
    get_summary_data(gapped, i, date_only, 'twelve_thirty_to_close', symbol, '12:30:00', '16:00:00')

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
      gapped.loc[i, 'market_cap'] = "{:.2f}".format(shares * float(row['close_yesterday']))

  gapped.to_csv(file_name_gap, index=False)




if __name__ == "__main__":

  # quote = get_stock_by_date('ASTC', '2021-03-12')
  # stats = get_stats('ASTC', '2021-03-12', '09:30:00', '16:00:00')

  # Will save finviz data for any stock that had over a million in volume
  # save_active_stocks_finviz_to_file()

  # find gap up instances, save to file
  # save_gap_up_data_to_summary_file()
  # # # # go through instance and find the high low stats
  # add_high_low_to_gap_up()
  # # # #add the finvis info to the gap up
  # add_finviz_to_gap_up()
  # add_booleans_to_gap_up()
  add_volume_to_gap_up()

  print('END')
