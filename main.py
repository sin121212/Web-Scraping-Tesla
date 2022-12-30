# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 10:51:13 2022

@author: kitsin
"""

import requests
import selenium

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

import time
import datetime

import pandas as pd
import numpy as np

import configparser

#%% Config

# C:\Users\chromedriver_win32\chromedriver.exe

config = configparser.ConfigParser()
config.read('config.ini')

#%% Tesla Website Scraping

from lib.tesla.all import TeslaWeb

tesla = TeslaWeb(url='https://www.tesla.com/en_jo/findus/list/superchargers/Hong+Kong',
                 driver=webdriver.Chrome(service=Service(config['file_path']['chrome_driver'])),
                 row_start=1,
                 row_end=config['tesla_web']['row'],
                 col_start=1,
                 col_end=config['tesla_web']['col'])

tesla_df = tesla.web_scraping()

#%%% Data Processing 

from lib.tesla.data_processing import DataProcess

dp = DataProcess(df=tesla_df,
                 lookup_table=config['file_path']['lookup_excel'])

get_lookup_final_adj_df = dp.get_lookup_final_adj()

dp_df = dp.dp_df
dp_df.to_excel('TeslaCharger.xlsx', index=False)

#%% Azure Data Warehouse Connection

from lib.azure.azure_con import AzureDataWarehouse

wp_dwh = AzureDataWarehouse(driver='ODBC Driver 17 for SQL Server',
                            username='',
                            password='',
                            host='',
                            database='')

wp_dwh_engine = wp_dwh.azure_db_engine()

wp_dwh_cursor = wp_dwh.azure_db_cursor()

#%%% Upload Data

from lib.export_to_adw.output import Output

output = Output(df=dp_df, 
                con=wp_dwh_engine,
                schema='dbo',
                table_name='TeslaChargerLocation')

exist_table = output.check_adw_table_exist()

output_df = output.excel_to_adw()

