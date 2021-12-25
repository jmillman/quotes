# -*- coding: utf-8 -*-
import pandas as pd
import time
import glob
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
import os
import re
from common import get_file, save_gap_up_data_to_summary_file, get_symbols_over_million_volume, get_data_finviz, scrape_yahoo_stats, check_dir, add_finviz_to_gap_up, get_summary_data, get_stats, save_gap_down_data_to_summary_file


directory_daily_history = "./stock_history/2021/daily"
file_name_gap = "./summary/gapped_up-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))


def add_pmh_day_to_gap_up():
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

    gapped = gapped[gapped['DATA_MISSING_5MIN'] != 1]
    gapped.to_csv(file_name_gap, index=False)



if __name__ == "__main__":

    # Will save finviz data for any stock that had over a million in volume
    # save_active_stocks_finviz_to_file()


    # add_high_low_to_gap_up()
    # add_finviz_to_gap_up()
    # add_volume_to_gap_up()
    # add_booleans_to_gap_up()

    start = datetime(2020, 10, 1).date()
    # save_gap_up_data_to_summary_file(start, 20)
    # add_pmh_day_to_gap_up()
    save_gap_down_data_to_summary_file(start, -49)