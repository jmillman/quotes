import pandas as pd
import glob
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os

# driver = webdriver.Chrome("./chromedriver")

directory_daily_history = "./stock_history_2021"
file_name_gap = "./summary/gapped_up-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))
file_name_finviz_summary = 'summary/finviz-{}.csv'.format(datetime.now().strftime("%Y-%m-%d"))


def get_file(symbol, date):
  if date > '2021-07-03':
    file_name_five_min = "./stock_history_minute_extended_2021_07_05_to_2021_CURRENT/{}.csv".format(symbol)
  elif date > '2021-05-28' and date <= '2021-07-02':
    file_name_five_min = "./stock_history_five_minute_extended_2021_06_01_to_2021_07_02/{}.csv".format(symbol)
  else:
    file_name_five_min = "./stock_history_five_minute_extended_2020_09_14_to_2021_05_28/{}.csv".format(symbol)
  return file_name_five_min


def save_gap_up_data_to_summary_file():
  files = sorted(glob.glob("{}/*.csv".format(directory_daily_history)))

  results = pd.DataFrame()
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
  file_name_five_min = get_file(symbol, date)

  low = -1
  high = -1
  low_time = ''
  high_time = ''
  close = -1
  close_time = ''
  open = -1
  open_time = ''
  stats_error = False
  try:
    daily_quotes = pd.read_csv(file_name_five_min)
    # get the time in EST
  except:
    print('{} file not found: {}  {}'.format(file_name_five_min, symbol, date))

  try:
    if len(daily_quotes):
      daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime'])
      daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_localize('UTC')
      daily_quotes['datetime'] = daily_quotes['datetime'].dt.tz_convert('US/Eastern')
      daily_quotes['time_only'] = daily_quotes['datetime'].dt.time

      # daily_quotes['datetime'] = pd.to_datetime(daily_quotes['datetime']) - timedelta(hours=4)
      # daily_quotes['date_only'] = daily_quotes['datetime'].dt.date

      start = '{} {}'.format(date, start_time)
      end = '{} {}'.format(date, end_time)

      day = daily_quotes[(daily_quotes['datetime'] >= start) & (daily_quotes['datetime'] <= end)]
      if len(day):
        low = day['low'].min()
        high = day['high'].max()
        close = day['close'].iloc[-1]
        close_time = day['time_only'].iloc[-1]
        open = day['open'].iloc[1]
        open_time = day['time_only'].iloc[1]

        if not pd.isna(low):
          low_time = day[day['low'] == low].iloc[0]['time_only']
          high_time = day[day['high'] == high].iloc[0]['time_only']
      else:
        print('Data Not found {} {} {} {}'.format(symbol, date, start_time, end_time))
        stats_error = True

  except Exception as e:
    print(e)

  stats = {
    'low': low,
    'high': high,
    'low_time': low_time,
    'high_time': high_time,
    'close': close,
    'close_time': close_time,
    'open': open,
    'open_time': open_time,
    'stats_error': stats_error
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
  if summary['stats_error']:
    gapped.loc[i, 'DATA_MISSING_STATS'] = True
    print('get_summary_data error {}'.format(symbol))
  else:
    add_columns(gapped, i, title, summary)


def add_bool_columns(gapped, name):
  percent_change_high = (gapped['{}_high'.format(name)] - gapped['{}_open'.format(name)]) / gapped['open']
  gapped['{}_percent_max_gain'.format(name)] = percent_change_high
  gapped['{}_percent_max_gain_gt_05p'.format(name)] = percent_change_high > .05
  gapped['{}_percent_max_gain_gt_10p'.format(name)] = percent_change_high > .10
  gapped['{}_percent_max_gain_gt_20p'.format(name)] = percent_change_high > .20
  gapped['{}_percent_max_gain_gt_50p'.format(name)] = percent_change_high > .50
  gapped['{}_percent_max_gain_gt_100p'.format(name)] = percent_change_high > 1

  percent_change_low = (gapped['{}_open'.format(name)] - gapped['{}_low'.format(name)]) / gapped['open']
  gapped['{}_percent_max_loss'.format(name)] = percent_change_low
  gapped['{}_percent_max_loss_gt_05p'.format(name)] = percent_change_low > .05
  gapped['{}_percent_max_loss_gt_10p'.format(name)] = percent_change_low > .10
  gapped['{}_percent_max_loss_gt_20p'.format(name)] = percent_change_low > .20
  gapped['{}_percent_max_loss_gt_50p'.format(name)] = percent_change_low > .50
  gapped['{}_percent_max_loss_gt_100p'.format(name)] = percent_change_low > 1


def add_booleans_to_gap_up():
  gapped = pd.read_csv(file_name_gap)

  add_bool_columns(gapped, 'fifteen_01')
  add_bool_columns(gapped, 'fifteen_02')
  add_bool_columns(gapped, 'fifteen_03')
  add_bool_columns(gapped, 'fifteen_04')
  add_bool_columns(gapped, 'fifteen_05')

  add_bool_columns(gapped, 'thirty_01')
  add_bool_columns(gapped, 'thirty_02')
  add_bool_columns(gapped, 'thirty_03')
  add_bool_columns(gapped, 'thirty_04')

  add_bool_columns(gapped, 'ten_thirty_to_close')
  add_bool_columns(gapped, 'eleven_thirty_to_close')
  add_bool_columns(gapped, 'twelve_thirty_to_close')

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
    file_name_five_min = get_file(symbol, date)

    if not os.path.isfile(file_name_five_min):
      print('File not found: {}'.format(file_name_five_min))
    else:
      try:
        daily_quotes = pd.read_csv(file_name_five_min)
        daily_quotes['date_only'] = pd.to_datetime(daily_quotes['datetime']).dt.date

        start = '{} {}'.format(date, "00:00:00")
        end = '{} {}'.format(date, "24:00:00")
        daily_quotes = daily_quotes[(daily_quotes['datetime'] >= start) & (daily_quotes['datetime'] <= end)]

        if row["float"] != 0:
          for five_min_index in range(37):
            time_stamp = datetime.strptime('{} 09:30:00'.format(date), '%Y-%m-%d %H:%M:%S') + (
              timedelta(minutes=5) * five_min_index)
            time_string = time_stamp.strftime('%Y-%m-%d %H:%M:%S')
            current_vol, total_vol = get_current_and_cum_volume(daily_quotes, time_string)
            gapped.loc[i, "f_{}_v_c".format(five_min_index)] = current_vol
            gapped.loc[i, "f_{}_v_t".format(five_min_index)] = total_vol
            gapped.loc[i, "f_{}_fr_c".format(five_min_index)] = current_vol / (row["float"] * 1000000)
            gapped.loc[i, "f_{}_fr_t".format(five_min_index)] = total_vol / (row["float"] * 1000000)

        else:
          gapped.loc[i, 'DATA_MISSING_FLOAT_ROTATION'] = True
          print('{} has 0 float'.format(symbol))
      except Exception as e:
        print(e)

  gapped.to_csv(file_name_gap, index=False)


def add_high_low_to_gap_up():
  gapped = pd.read_csv(file_name_gap)
  for i, row in gapped.iterrows():
    symbol = row['symbol']
    date_only = row['date_only']

    file_name_five_min = get_file(symbol, date_only)
    if not os.path.isfile(file_name_five_min):
      gapped.loc[i, 'DATA_MISSING_5MIN'] = True
      print('File not found: {}'.format(file_name_five_min))
    else:
      get_summary_data(gapped, i, date_only, 'pre', symbol, '07:30:00', '09:30:00')
      get_summary_data(gapped, i, date_only, 'daily', symbol, '09:30:00', '16:00:00')

      get_summary_data(gapped, i, date_only, 'fifteen_01', symbol, '09:30:00', '9:45:00')
      get_summary_data(gapped, i, date_only, 'fifteen_02', symbol, '09:45:00', '10:00:00')
      get_summary_data(gapped, i, date_only, 'fifteen_03', symbol, '10:00:00', '10:15:00')
      get_summary_data(gapped, i, date_only, 'fifteen_04', symbol, '10:15:00', '10:30:00')
      get_summary_data(gapped, i, date_only, 'fifteen_05', symbol, '10:30:00', '10:45:00')

      get_summary_data(gapped, i, date_only, 'thirty_01', symbol, '09:30:00', '10:00:00')
      get_summary_data(gapped, i, date_only, 'thirty_02', symbol, '10:00:00', '10:30:00')
      get_summary_data(gapped, i, date_only, 'thirty_03', symbol, '10:30:00', '11:00:00')
      get_summary_data(gapped, i, date_only, 'thirty_04', symbol, '11:00:00', '11:30:00')

      get_summary_data(gapped, i, date_only, 'hour_01', symbol, '09:30:00', '10:30:00')
      get_summary_data(gapped, i, date_only, 'hour_02', symbol, '10:30:00', '11:30:00')
      get_summary_data(gapped, i, date_only, 'hour_03', symbol, '11:30:00', '12:30:00')

      get_summary_data(gapped, i, date_only, 'ten_thirty_to_close', symbol, '10:30:00', '16:00:00')
      get_summary_data(gapped, i, date_only, 'eleven_thirty_to_close', symbol, '11:30:00', '16:00:00')
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
  file_name_finviz = "./summary/finviz-2021-07-03.csv"
  gapped = pd.read_csv(file_name_gap)
  finviz = pd.read_csv(file_name_finviz)
  for i, row in gapped.iterrows():
    symbol = row['symbol']
    finviz_item = finviz[finviz['Symbol'] == symbol]
    if (len(finviz_item)):
      gapped.loc[i, 'float'] = convert_billion_to_mill(finviz_item.iloc[0]['Shs Float'])
      shares = convert_billion_to_mill(finviz_item.iloc[0]['Shs Outstand'])
      gapped.loc[i, 'shares'] = shares
      gapped.loc[i, 'market_cap'] = "{:.2f}".format(shares * float(row['close_yesterday']))
    else:
      gapped.loc[i, 'DATA_MISSING_FINVIZ'] = True
      print('Finviz missing symbol: {}'.format(symbol))

  gapped.to_csv(file_name_gap, index=False)


if __name__ == "__main__":
  # quote = get_stock_by_date('ASTC', '2021-03-12')
  # stats = get_stats('ASTC', '2021-03-12', '09:30:00', '16:00:00')

  # Will save finviz data for any stock that had over a million in volume
  # save_active_stocks_finviz_to_file()

  # find gap up instances, save to file
  save_gap_up_data_to_summary_file()
  # # # # # # go through instance and find the high low stats
  add_high_low_to_gap_up()
  # # # # # #add the finvis info to the gap up
  add_finviz_to_gap_up()
  add_volume_to_gap_up()
  add_booleans_to_gap_up()

  print('END')
