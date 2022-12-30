# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 16:36:01 2022

@author: kitsin
"""

import requests
import selenium

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from selenium.webdriver import ActionChains

import time
import datetime

import pandas as pd
import numpy as np


#%%
class TeslaWeb:
    
    def __init__(self, url, driver, row_start, row_end, col_start, col_end):
        
        self.url = url
        self.driver = driver
        
        self.row_start = row_start
        self.row_end = int(row_end)
        
        self.col_start = col_start
        self.col_end = int(col_end)
        
        pass

    def web_scraping(self):
        
        start = time.time()
        
        def download_dataframe():
            
            '''
            row 1 column 1
                name: /html/body/section/div/div/section/div/div[1]/address[1]/a
                
                address: /html/body/section/div/div/section/div/div[1]/address[1]/span[1]/span[1]
                district: /html/body/section/div/div/section/div/div[1]/address[1]/span[1]/span[3]
                page: /html/body/section/div/div/section/div/div[1]/address[1]/a
                
                
            row 1 column 2
                name: /html/body/section/div/div/section/div/div[1]/address[2]/span[1]/span[1]
                
            row 2 column 1
                name: /html/body/section/div/div/section/div/div[2]/address[1]/span[1]/span[1]
                
            Detail page            
                /html/body/section/div/div/section/address/p[2]
                
            '''
            
            
            
            self.driver.get(self.url)
            time.sleep(3)
            
            # click close menu button
            try:
                close = self.driver.find_element(By.XPATH , '/html/body/header/div/dialog/div/button')
                close.click()
            except:
              pass
            time.sleep(2)
                
            # row and column number list
            row_list = list(range(self.row_start, self.row_end + 1))
            # col_list = list(range(1, 3 + 1))
            col_list = list(range(self.col_start, self.col_end + 1))

            # storage
            name_list = []
            eng_address_list = []
            district_list = []
            detail_list = []
            chinese_address_list = []
            
            for row in row_list:
                
                for col in col_list:
                    
                    print(f'row: {row}, col: {col}')
                    
                    # name
                    try:
                        name = self.driver.find_element(By.XPATH, f'/html/body/section/div/div/section/div/div[{row}]/address[{col}]/a')
                        name_text = name.text
                    except:
                        name_text = ''
                    print(f'name: {name_text}')   
                    name_list.append(name_text)
                    
                    # address
                    try:
                        eng_address = self.driver.find_element(By.XPATH, f'/html/body/section/div/div/section/div/div[{row}]/address[{col}]/span[1]/span[1]')
                        eng_address_text = eng_address.text
                    except:
                        eng_address_text = ''
                    print(f'address: {eng_address_text}')
                    eng_address_list.append(eng_address_text)
                    
                    # district
                    try:
                        district = self.driver.find_element(By.XPATH, f'/html/body/section/div/div/section/div/div[{row}]/address[{col}]/span[1]/span[3]')
                        district_text = district.text
                    except:
                        district_text = ''
                    print(f'district: {district_text}')
                    district_list.append(district_text)
                                  
                    # open charger detail page
                    try:
                        # if target row/col invalid, seted the name = None in above code
                        detail_page = self.driver.find_element(By.XPATH, f'/html/body/section/div/div/section/div/div[{row}]/address[{col}]/a')
                        time.sleep(1.5)
                        
                        # scroll down
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", detail_page)
                        time.sleep(1.5)
                        
                        # click page
                        detail_page.click()
                        time.sleep(1.5)
                        
                        try:
                            # get detail
                            detail = self.driver.find_element(By.XPATH , '/html/body/section/div/div/section/address/p[2]')
                            detail_text = detail.text
                        except:
                            detail_text = ''
                        
                        time.sleep(0.5)
                        
                        try:
                            # get chinese address
                            chinese_address = self.driver.find_element(By.XPATH , '/html/body/section/div/div/section/address/span[2]')
                            chinese_address_text = chinese_address.text
                        except:
                            chinese_address_text = ''
                        
                        time.sleep(0.5)
                        
                        self.driver.back()
                    
                    except:
                        detail_text = ''
                        chinese_address_text = ''
                        
                    print(f'detail: {detail_text}')
                    detail_list.append(detail_text)
                    print(f'chinese_address: {chinese_address_text}')
                    chinese_address_list.append(chinese_address_text)
                    
                    time.sleep(2)
                    
                    print('')
                    # end of loop
        
            data = {'Name': name_list,
                    'EnglishAddress': eng_address_list,
                    'ChineseAddress': chinese_address_list,
                    'District': district_list,
                    'Detail': detail_list}
            
            df = pd.DataFrame(data)

            return df
            
        df = (download_dataframe()
              )
        
        print('It took {0:0.1f} seconds'.format(time.time() - start))
        
        return df

#%%




