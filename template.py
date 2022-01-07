# -*- coding: utf-8 -*-
import pandas as pd
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os
import re
from lib.quote_functions import get_summary_data, get_file, save_summary_to_file, add_finviz_to_gap_up

def add_time_slices(file_name):
    gapped = pd.read_csv(file_name)
    for i, row in gapped.iterrows():
        symbol = row['symbol']
        date_only = row['date_only']

        file_name_five_min = get_file(symbol, date_only)
        if not os.path.isfile(file_name_five_min):
            gapped.loc[i, 'DATA_MISSING_5MIN'] = 1
            print('File not found: {}'.format(file_name_five_min))
        else:
            get_summary_data(gapped, i, date_only, 'daily', symbol, '09:30:00', '16:00:00')
            get_summary_data(gapped, i, date_only, 'first_15', symbol, '09:30:00', '09:45:00')
            get_summary_data(gapped, i, date_only, '945_close', symbol, '09:45:00', '16:00:00')

    gapped.to_csv(file_name, index=False)

def add_booleans(file_name):
    gapped = pd.read_csv(file_name)
    gapped['max_down_15_close'] = (gapped['first_15_close'] - gapped['945_close_low']) / gapped['first_15_close']
    gapped['max_gain_15_close'] = (gapped['945_close_high'] - gapped['first_15_close']) / gapped['first_15_close']
    gapped['first_15_low_is_lowest_of_day'] = gapped['first_15_low'] > gapped['945_close_low']
    gapped.to_csv(file_name, index=False)

if __name__ == "__main__":
    # You can set an end date if you wish
    start = datetime(2021, 1, 1).date()
    end = datetime(2021, 2, 1).date()
    # end = None

    # most parameters are optional, you can pass in a percent down or a percent up, also volume
    # file_name = "gap_down_40-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))
    # save_summary_to_file(start_date=start, end_date=end, percent_down=-40, file_name=file_name)

    file_name = "./summary/gap_up_40-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))
    save_summary_to_file(start_date=start, end_date=end, percent_up=40, file_name=file_name)
    add_time_slices(file_name=file_name)
    add_booleans(file_name=file_name)

    file_name_finviz = "stock_history/finviz/finviz-2021-10-30.csv"
    add_finviz_to_gap_up(file_name_finviz, file_name)

