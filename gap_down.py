# -*- coding: utf-8 -*-
import pandas as pd
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os
import re
from main import save_gap_down_data_to_summary_file
from common import get_summary_data, get_file

directory_daily_history = "./stock_history/2021/daily"
file_name_gap = "./summary/gapped_up-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))


def add_time_slices():
    gapped = pd.read_csv(file_name_gap)
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

    gapped.to_csv(file_name_gap, index=False)


def add_booleans():
    gapped = pd.read_csv(file_name_gap)
    gapped['max_down_15_close'] = (gapped['first_15_close'] - gapped['945_close_low']) / gapped['first_15_close']
    gapped['max_gain_15_close'] = (gapped['945_close_high'] - gapped['first_15_close']) / gapped['first_15_close']
    gapped['first_15_low_is_lowest_of_day'] = gapped['first_15_low'] > gapped['945_close_low']
    gapped.to_csv(file_name_gap, index=False)




if __name__ == "__main__":
    # find gap up instances, save to file
    # start = datetime(2021, 1, 1).date()
    # save_gap_down_data_to_summary_file(start, -30)
    # add_time_slices()
    add_booleans()

