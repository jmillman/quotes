# -*- coding: utf-8 -*-
import pandas as pd
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os
import re


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

def save_active_stocks_finviz_to_file(update_only_missing_symbols=False):
    file_name_finviz = "./summary/finviz-2021-10-30.csv"
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
        df.to_csv(file_name_finviz, mode='a+', header=showHeaders, index=False)


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
    high_touch = 1
    low_touch = 1
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
                    low_touch = len(day[day['low'] == low])
                    high_time = day[day['high'] == high].iloc[0]['time_only']
                    high_touch = len(day[day['high'] == high])
            else:
                print('Data Not found {} {} {} {}'.format(symbol, date, start_time, end_time))
                stats_error = True

    except Exception as e:
        print(e)

    stats = {
        'low': low,
        'high': high,
        'low_time': low_time,
        'low_touch': low_touch,
        'high_time': high_time,
        'high_touch': high_touch,
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
    gapped.loc[i, '{}_high_time'.format(title)] = stats_obj['high_time']
    gapped.loc[i, '{}_high_touch'.format(title)] = stats_obj['high_touch']
    gapped.loc[i, '{}_low'.format(title)] = stats_obj['low']
    gapped.loc[i, '{}_low_time'.format(title)] = stats_obj['low_time']
    gapped.loc[i, '{}_low_touch'.format(title)] = stats_obj['low_touch']
    gapped.loc[i, '{}_close'.format(title)] = stats_obj['close']
    # gapped.loc[i, '{}_close_time'.format(title)] = stats_obj['close_time']


def get_summary_data(gapped, i, date_only, title, symbol, start_time, end_time):
    summary = get_stats(symbol, date_only, start_time, end_time)
    if summary['stats_error']:
        # gapped.drop(i, inplace=True)
        gapped.loc[i, 'DATA_MISSING_STATS'] = True
        print('get_summary_data error {}'.format(symbol))
    else:
        add_columns(gapped, i, title, summary)


# percent_change_high = (gapped['{}_high'.format(name)] - gapped['{}_open'.format(name)]) / gapped['open']
def add_bool_columns(gapped, name):
    percent_change_high = (gapped['{}_high'.format(name)] - gapped['{}_open'.format(name)]) / gapped[
        '{}_open'.format(name)]  # changed the denominator for slots
    gapped['{}_percent_max_gain'.format(name)] = percent_change_high
    gapped['{}_percent_max_gain_gt_05p'.format(name)] = percent_change_high > .05
    gapped['{}_percent_max_gain_gt_10p'.format(name)] = percent_change_high > .10
    gapped['{}_percent_max_gain_gt_20p'.format(name)] = percent_change_high > .20
    gapped['{}_percent_max_gain_gt_50p'.format(name)] = percent_change_high > .50
    gapped['{}_percent_max_gain_gt_100p'.format(name)] = percent_change_high > 1

    percent_change_low = (gapped['{}_open'.format(name)] - gapped['{}_low'.format(name)]) / gapped[
        '{}_open'.format(name)]
    gapped['{}_percent_max_loss'.format(name)] = percent_change_low
    gapped['{}_percent_max_loss_gt_05p'.format(name)] = percent_change_low > .05
    gapped['{}_percent_max_loss_gt_10p'.format(name)] = percent_change_low > .10
    gapped['{}_percent_max_loss_gt_20p'.format(name)] = percent_change_low > .20
    gapped['{}_percent_max_loss_gt_50p'.format(name)] = percent_change_low > .50
    gapped['{}_percent_max_loss_gt_100p'.format(name)] = percent_change_low > 1


def add_booleans_to_gap_up():
    gapped = pd.read_csv(file_name_gap)

    add_bool_columns(gapped, 'fifteen_00')
    add_bool_columns(gapped, 'fifteen_01')
    add_bool_columns(gapped, 'fifteen_02')
    add_bool_columns(gapped, 'fifteen_03')
    add_bool_columns(gapped, 'fifteen_04')
    add_bool_columns(gapped, 'fifteen_05')
    add_bool_columns(gapped, 'fifteen_06')
    add_bool_columns(gapped, 'fifteen_07')
    add_bool_columns(gapped, 'fifteen_08')
    add_bool_columns(gapped, 'fifteen_09')
    # add_bool_columns(gapped, 'fifteen_10')

    add_bool_columns(gapped, 'thirty_00')
    add_bool_columns(gapped, 'thirty_01')
    add_bool_columns(gapped, 'thirty_02')
    add_bool_columns(gapped, 'thirty_03')

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
            gapped.loc[i, 'DATA_MISSING_5MIN'] = 1
            print('File not found: {}'.format(file_name_five_min))
        else:
            get_summary_data(gapped, i, date_only, 'pre', symbol, '07:30:00', '09:30:00')
            get_summary_data(gapped, i, date_only, 'daily', symbol, '09:30:00', '16:00:00')

            get_summary_data(gapped, i, date_only, 'fifteen_00', symbol, '09:30:00', '9:45:00')
            get_summary_data(gapped, i, date_only, 'fifteen_01', symbol, '09:45:00', '10:00:00')
            get_summary_data(gapped, i, date_only, 'fifteen_02', symbol, '10:00:00', '10:15:00')
            get_summary_data(gapped, i, date_only, 'fifteen_03', symbol, '10:15:00', '10:30:00')
            get_summary_data(gapped, i, date_only, 'fifteen_04', symbol, '10:30:00', '10:45:00')
            get_summary_data(gapped, i, date_only, 'fifteen_05', symbol, '10:30:00', '10:45:00')
            get_summary_data(gapped, i, date_only, 'fifteen_06', symbol, '10:45:00', '11:00:00')
            get_summary_data(gapped, i, date_only, 'fifteen_07', symbol, '11:00:00', '11:15:00')
            get_summary_data(gapped, i, date_only, 'fifteen_08', symbol, '11:15:00', '11:30:00')
            get_summary_data(gapped, i, date_only, 'fifteen_09', symbol, '11:30:00', '11:45:00')

            get_summary_data(gapped, i, date_only, 'thirty_00', symbol, '09:30:00', '10:00:00')
            get_summary_data(gapped, i, date_only, 'thirty_01', symbol, '10:00:00', '10:30:00')
            get_summary_data(gapped, i, date_only, 'thirty_02', symbol, '10:30:00', '11:00:00')
            get_summary_data(gapped, i, date_only, 'thirty_03', symbol, '11:00:00', '11:30:00')

            get_summary_data(gapped, i, date_only, 'hour_01', symbol, '09:30:00', '10:30:00')
            get_summary_data(gapped, i, date_only, 'hour_02', symbol, '10:30:00', '11:30:00')
            get_summary_data(gapped, i, date_only, 'hour_03', symbol, '11:30:00', '12:30:00')

            get_summary_data(gapped, i, date_only, 'ten_thirty_to_close', symbol, '10:30:00', '16:00:00')
            get_summary_data(gapped, i, date_only, 'eleven_thirty_to_close', symbol, '11:30:00', '16:00:00')
            get_summary_data(gapped, i, date_only, 'twelve_thirty_to_close', symbol, '12:30:00', '16:00:00')

    gapped.to_csv(file_name_gap, index=False)


def add_high_low_time_to_gap_up():
    gapped = pd.read_csv(file_name_gap)
    for i, row in gapped.iterrows():
        symbol = row['symbol']
        date_only = row['date_only']

        file_name_five_min = get_file(symbol, date_only)
        if not os.path.isfile(file_name_five_min):
            # gapped.drop(i, inplace=True)
            gapped.loc[i, 'DATA_MISSING_5MIN'] = 1
            print('File not found: {}'.format(file_name_five_min))
        else:
            get_summary_data(gapped, i, date_only, 'pre_market', symbol, '07:30:00', '09:30:00')
            get_summary_data(gapped, i, date_only, 'day', symbol, '09:30:00', '16:00:00')

            get_summary_data(gapped, i, date_only, '930_935', symbol, '09:30:00', '9:35:00')
            get_summary_data(gapped, i, date_only, '935_940', symbol, '09:35:00', '9:40:00')
            get_summary_data(gapped, i, date_only, '930_945', symbol, '09:30:00', '9:45:00')
            get_summary_data(gapped, i, date_only, '945_950', symbol, '09:45:00', '09:50:00')
            get_summary_data(gapped, i, date_only, '945_955', symbol, '09:45:00', '09:55:00')
            get_summary_data(gapped, i, date_only, '945_10', symbol, '09:45:00', '10:00:00')
            get_summary_data(gapped, i, date_only, '945_4', symbol, '09:45:00', '16:00:00')

            get_summary_data(gapped, i, date_only, '930_10', symbol, '09:30:00', '10:00:00')
            get_summary_data(gapped, i, date_only, '10_4', symbol, '10:00:00', '16:00:00')

            get_summary_data(gapped, i, date_only, '930_1030', symbol, '09:30:00', '10:30:00')
            get_summary_data(gapped, i, date_only, '1030_4', symbol, '10:30:00', '16:00:00')

            get_summary_data(gapped, i, date_only, '930_11', symbol, '09:30:00', '11:00:00')
            get_summary_data(gapped, i, date_only, '11_4', symbol, '11:00:00', '16:00:00')
            get_summary_data(gapped, i, date_only, '1130_4', symbol, '11:30:00', '16:00:00')
            get_summary_data(gapped, i, date_only, '12_4', symbol, '12:00:00', '16:00:00')

    gapped = gapped[gapped['DATA_MISSING_5MIN'] != 1]
    gapped.to_csv(file_name_gap, index=False)


def convert_billion_to_mill(str):
    if str == '-' or pd.isna(str):
        return 0
    elif 'k' in str:
        return float(str.replace("k", "")) / 1000
    elif 'B' in str:
        return float(str.replace("B", "")) * 1000
    else:
        return float(str.replace("M", ""))


def add_finviz_to_gap_up():
    file_name_finviz = "./summary/finviz-2021-10-30.csv"
    gapped = pd.read_csv(file_name_gap)
    finviz = pd.read_csv(file_name_finviz)
    for i, row in gapped.iterrows():
        missingData = False
        symbol = row['symbol']
        finviz_item = finviz[finviz['Symbol'] == symbol]

        if (len(finviz_item)):

            # If no float, but shares use shares for float and vice versa
            # typically it's really low shares outstanding so the float has to be less than that
            float_shrs = finviz_item.iloc[0]['Shs Float']
            if float_shrs in ['Not Found', 'N/A', '-']:
                float_shrs = 0
                missingData = True

            shares = finviz_item.iloc[0]['Shs Outstand']
            if shares in ['Not Found', 'N/A', '-']:
                missingData = True
                shares = float_shrs

            if float_shrs == 0:
                float_shrs = shares

            if(float_shrs != 0):
                float_shrs = convert_billion_to_mill(float_shrs)
            if(shares != 0):
                shares = convert_billion_to_mill(shares)
            gapped.loc[i, 'float'] = shares
            gapped.loc[i, 'shares'] = shares
            gapped.loc[i, 'market_cap'] = "{:.2f}".format(shares * float(gapped.loc[i, 'close_yesterday']))
        else:
            missingData = True
            print('Finviz missing symbol: {}'.format(symbol))

        if missingData == True:
            gapped.loc[i, 'DATA_MISSING_FINVIZ'] = True

    gapped.to_csv(file_name_gap, index=False)


def add_bool_columns_new(gapped, name):
    gapped['{}_never_broke_pre_high'.format(name)] = gapped['{}_high'.format(name)] <= gapped['pre_market_high']
    gapped['{}_never_broke_post_high'.format(name)] = gapped['{}_high'.format(name)] <= gapped['930_945_high']
    gapped['{}_always_gt_open'.format(name)] = gapped['{}_low'.format(name)] >= gapped['day_open']
    gapped['{}_always_lt_open'.format(name)] = gapped['{}_high'.format(name)] <= gapped['day_open']
    gapped['{}_crosses_open'.format(name)] = (gapped['{}_low'.format(name)] <= gapped['day_open']) & (
            gapped['{}_high'.format(name)] >= gapped['day_open'])
    percent_change_high = (gapped['{}_high'.format(name)] - gapped['{}_open'.format(name)]) / gapped[
        '{}_open'.format(name)]  # changed the denominator for slots
    gapped['{}_closed_up'.format(name)] = (gapped['{}_close'.format(name)] > gapped['{}_open'.format(name)])
    gapped['{}_max_hi'.format(name)] = percent_change_high
    gapped['{}_max_hi_10p'.format(name)] = percent_change_high >= .10
    gapped['{}_max_hi_25p'.format(name)] = percent_change_high >= .25
    gapped['{}_max_hi_50p'.format(name)] = percent_change_high >= .50
    gapped['{}_max_hi_100p'.format(name)] = percent_change_high >= 1

    percent_change_low = (gapped['{}_open'.format(name)] - gapped['{}_low'.format(name)]) / gapped[
        '{}_open'.format(name)]
    gapped['{}_max_low'.format(name)] = percent_change_low
    gapped['{}_max_low_10p'.format(name)] = percent_change_low >= .10
    gapped['{}_max_low_25p'.format(name)] = percent_change_low >= .25
    gapped['{}_max_low_50p'.format(name)] = percent_change_low >= .50

    # gapped['{}_percent_both_10p'.format(name)] = bool(percent_change_high >= .10) and bool(percent_change_low >= .10)
    # gapped['{}_percent_both_25p'.format(name)] = bool(percent_change_high >= .25) and bool(percent_change_low >= .25)
    # gapped['{}_percent_both_50p'.format(name)] = bool(percent_change_high >= .50) and bool(percent_change_low >= .50)


def add_booleans_new():
    gapped = pd.read_csv(file_name_gap)

    volume = gapped['volume']
    gapped['volume'] = gapped['volume'].astype(float)
    gapped['float'] = gapped['float'].astype(float)

    gapped['first_bar_up'] = gapped['930_935_close'] >= gapped['930_935_open']
    gapped['second_bar_up'] = gapped['935_940_close'] >= gapped['935_940_open']
    gapped['both_bars_up'] = gapped['first_bar_up'] & gapped['second_bar_up']

    gapped['rotate'] = gapped['volume'] < (gapped['float'] * 1000000)
    gapped['rotate_count'] = gapped['volume'] / (gapped['float'] * 1000000)

    slices = ['pre_market', 'day', '930_945', '945_950', '945_955', '945_10', '945_4', '930_10', '10_4', '930_1030',
              '1030_4', '930_11', '11_4', '1130_4', '12_4']
    for slice in slices:
        add_bool_columns_new(gapped, slice)

    gapped.to_csv(file_name_gap, index=False)


def calculate_stats_from_booleans():
    gapped = pd.read_csv(file_name_gap)
    total = len(gapped)
    df = pd.DataFrame(columns=['title', 'percent_true'])

    newRow = {'title': 'total', 'percent_true': total}
    df = df.append(newRow, ignore_index=True)

    for col in gapped.columns:
        if (gapped.dtypes[col] == 'bool'):
            matching = len(gapped[gapped[col] == True])
            percent_true = int(matching / total * 100)
            newRow = {'title': col, 'percent_true': percent_true}
            df = df.append(newRow, ignore_index=True)
            print('{} {}'.format(col, percent_true))

    count = 0
    while count < 10:
        file_name = "./summary/summary-{}-{}.csv".format(datetime.now().strftime("%Y-%m-%d"), count)
        if not os.path.isfile(file_name):
            df.to_csv(file_name, index=False)
            count = 10
        count += 1

def check_dir(directory_name):
  if not os.path.exists(directory_name):
    os.makedirs(directory_name)

# This is the code I used to split the files into the new format by month
# def reclassify_files():
#     type = 'minute'
#     # stock_folder = 'stock_history_five_minute_extended_2020_09_14_to_2021_05_28'
#     folders = sorted(glob.glob("./*history_minute*"))
#     for folder in folders:
#         files = sorted(glob.glob("{}/*.csv".format(folder)))
#         for i in range(len(files)):
#             symbol = files[i].split('/')[2].split('.')[0]
#             quote_file = pd.read_csv(files[i])
#             # quote_file = quote_file[quote_file['datetime'].str.startswith('2021')]
#
#             # This will give year-month like 2021-09
#             dates = sorted(set(quote_file['datetime'].str.split(' ', expand=True)[0].str[:-3]))
#             for date in dates:
#                 year, month = date.split('-')
#                 filename = './stock_history/{}'.format(year)
#                 check_dir(filename)
#                 filename = './stock_history/{}/{}/'.format(year, type)
#                 check_dir(filename)
#                 filename = './stock_history/{}/{}/{}'.format(year, type, month)
#                 check_dir(filename)
#                 filename = './stock_history/{}/{}/{}/{}.csv'.format(year, type, month, symbol)
#
#                 # don't do 2021, since that is when I will start pulling that way
#                 if year == '2021' and month == '10':
#                     pass
#                 else:
#                     dates_worth_of_quotes = quote_file[quote_file['datetime'].str.startswith(date)]
#                     showHeader = not os.path.isfile(filename)
#                     dates_worth_of_quotes.to_csv(filename, mode='a+', header=showHeader, index=False)
#
#                 print('{} {}'.format(symbol, date))


if __name__ == "__main__":

    # Will save finviz data for any stock that had over a million in volume
    # save_active_stocks_finviz_to_file()

    # find gap up instances, save to file
    # start = datetime(2021, 1, 1).date()
    # save_gap_up_data_to_summary_file(start, 30)
    # add_high_low_to_gap_up()
    # add_finviz_to_gap_up()
    # add_volume_to_gap_up()
    # add_booleans_to_gap_up()

    start = datetime(2021, 1, 1).date()
    save_gap_up_data_to_summary_file(start, 20)
    # add_finviz_to_gap_up()
    add_high_low_time_to_gap_up()
    # add_booleans_new()
    # calculate_stats_from_booleans()



    # save_active_stocks_finviz_to_file()

    # driver_yahoo = None
    # driver = None
    print('END')
