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
file_name_gap = "./summary/gapped_up-{}.csv".format(datetime.now().strftime("%Y-%m-%d"))





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