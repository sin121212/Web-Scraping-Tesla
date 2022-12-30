# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 09:20:16 2022

@author: kitsin
"""

import time
import datetime

import pandas as pd
import numpy as np
import re

import requests
import selenium

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

#%% Data Processing

class DataProcess:
    
    def __init__(self, df, lookup_table):
        
        self.df = df
        self.lookup_table = lookup_table
        
        self.dp_df = self.tesla_pipeline()
        
        pass
                
    def get_lookup_gov_district(self):
        
        df = pd.read_excel(self.lookup_table, sheet_name='District')
        
        return df
    
    def get_lookup_final_adj(self):
        
        df = pd.read_excel(self.lookup_table, sheet_name='FinalAdj')
        
        return df
    
    def get_append_kilowatt(self):
        
        df = pd.read_excel(self.lookup_table, sheet_name='Kilowatt')
        
        return df
    
    def tesla_pipeline(self):
        
        def exclude_record(df):
            
            df = df[df['Name'] != '']
            
            # exclude coming soon
            df = df[~df['Name'].str.contains('coming')]

            return df
        
        def split_eng_chinese_name(df):
            
            def fun(mix_str):
                
                '''
                mix_str: Citygate N Supercharger 東薈城 N 超級充電站
                '''
                
                # print(mix_str)
                
                try:
                
                    chinese_cha_list = []
                    
                    # find all chinese character
                    for n in re.findall(r'[\u4e00-\u9fff]+', mix_str):
                        
                        chinese_cha_list.append(n)
                        
                    # concat list of chinese character
                    chinese_cha = ''.join(chinese_cha_list)
                    
                    # first chinese character
                    first_chinese_cha = chinese_cha[0]
                    
                    # find position of first chinese character and then split
                    split_at = mix_str.find(first_chinese_cha)
        
                    eng, chinese = mix_str[:split_at], mix_str[split_at:]
                    
                except:
                    
                    # if not find chinese character, classify the mix_str as english name
                    eng, chinese = mix_str, ''
                
                return pd.Series([eng, chinese])
            
            df[['EnglishName', 'ChineseName']] = df['Name'].apply(fun)
            
            df = df.drop(['Name'], axis=1)
                    
            # split_eng_chinese_name
            return df
        
        def trim_name(df):
            
            name_col_list = ['EnglishName', 'ChineseName']
            
            df[name_col_list] = df[name_col_list].apply(lambda x: x.str.strip())
            
            return df
        
        def adjust_chinese_name(df):
            
            df['ChineseName'] = np.where(df['ChineseName'] == '超級充電站',
                                         df['EnglishName'] + df['ChineseName'],
                                         df['ChineseName'])
            
            return df
                
        def charger_number_tesla_web(df):
            
            '''
            Get the number of v3 charger
            '''
            
            def v3_fun(detail_str):
                
                # return a list of string, which split column 'detail' by \n (new line)
                str_list = detail_str.split('\n')
                # print(str_list)
                
                match_list = []
     
                for i in str_list:
                    if '250kW' in i:
                        match_list.append(i)
                # print(f'match_list: {match_list}')
                
                # list of string to text
                text = ' '.join(match_list)
                # print(f'text: {text}')
                
                split_at = text.find(' Superchargers')
                
                num = text[:split_at]
                
                # print(f'num: {num}')
                
                return num
            
            df['Number'] = df['Detail'].apply(v3_fun)
            
            df['Number'] = pd.to_numeric(df['Number'], errors='coerce')
            
            df['Number'] = df['Number'].fillna(0)
            
            df['ChargerType'] = 'V3'
            
            df['Source'] = 'Tesla Website'
            
            # charger_number
            return df
                
        def adjustment_district(df):
            
            df['District'] = df['District'].str.replace('Tsimshatsui', 'Tsim Sha Tsui')
            
            return df
        
        def grouping_by_address(df):
            
            '''
            Location grouping (Address, EnglishName, ChineseName)
            
                Cyberport, 數碼港
                Olympian City, 奧海城 (grouped)
                YOHO, 形點
                Elements, 圓方
            
            '''
            
            def grouping_fun(df, if_eng_address_contain, then_adj_eng_address, then_adj_chi_address, assign_eng_name, assign_chi_name):
                
                '''
                If address contain keyword, then assign same address for those record.
                Then based on the adjusted address, assign same Eng & Chinese name.
                '''

                df['EnglishAddress'] = np.where(df['EnglishAddress'].str.contains(if_eng_address_contain),
                                                then_adj_eng_address,
                                                df['EnglishAddress'])
                
                df['ChineseAddress'] = np.where(df['EnglishAddress'].str.contains(if_eng_address_contain),
                                                then_adj_chi_address,
                                                df['ChineseAddress'])
                
                df['EnglishName'] = np.where(df['EnglishAddress'] == then_adj_eng_address,
                                             assign_eng_name, 
                                             df['EnglishName'])
                
                df['ChineseName'] = np.where(df['EnglishAddress'] == then_adj_eng_address,
                                             assign_chi_name,
                                             df['ChineseName'])
                
                return df
            
            df = grouping_fun(df=df,
                              if_eng_address_contain='Cyberport',
                              then_adj_eng_address='100 Cyberport Rd',
                              then_adj_chi_address='100 數碼港道',
                              assign_eng_name='Cyberport Supercharger',
                              assign_chi_name='數碼港超級充電站')
            
            df = grouping_fun(df=df,
                              if_eng_address_contain='9 Long Yat Rd|Yuen Lung Street',
                              then_adj_eng_address='Yuen Lung Street and 9 Long Yat Rd',
                              then_adj_chi_address='元朗元龍街和朗日路9號',
                              assign_eng_name='Yoho Mall Supercharger',
                              assign_chi_name='形點超級充電站')
            
            df = grouping_fun(df=df,
                              if_eng_address_contain='1 Austin',
                              then_adj_eng_address='1 Austin Road West',
                              then_adj_chi_address='1號 柯士甸道西',
                              assign_eng_name='Elements Supercharger',
                              assign_chi_name='圓方超級充電站')

            return df
        
        def chinese_address_step1(df):
            
            def fun(string):
                
                '''
                Example of string:
                    Austin \n
                    柯士甸
                '''
                
                # return list, split by new line
                str_list = string.split('\n')
                
                # get the last line string
                chinese_address = str_list[-1]
                
                return chinese_address
            
            df['ChineseAddress'] = df['ChineseAddress'].apply(fun)
            
            # chinese_address_step1
            return df
        
        def chinese_address_step2(df):
            
            def fun(string):
                
                '''
                Example of string:
                    8 重華路 -> 重華路8號
                    
                    if split by space: ['8', '重華路'] <- list with length = 2
                    
                '''
                # print(string)
                
                # return list, split by space
                str_list = string.split(' ')
                
                # print(f'len: {len(str_list)}')
                
                if ( (len(str_list) == 2) & (str_list[0].isnumeric()) ):
                    # example: ['8', '重華路']
                    adj_add = str_list[1] + str_list[0] + '號'
                elif ( (len(str_list) == 2) & ('號' in str_list[0]) ):
                    # example: ['8號', '重華路']
                    adj_add = str_list[1] + str_list[0]
                else:
                    adj_add = string

                return adj_add
            
            df['ChineseAddress'] = df['ChineseAddress'].apply(fun)
            
            # chinese_address_step2
            return df
        
        def adj_district(df):
            
            '''
            Adjust website district before mapping
            
                Cyberport ->	Pok Fu Lam
                Kai Tak Ferry Terminal ->	Kowloon Bay
                Tsueng Kwan O ->	Tseung Kwan O
                Heng Fa Chuen ->	Chai Wan
                Soho	-> Central and Western
            
            '''
            
            # adjust district by tesla web district
            df['District'] = df['District'].str.replace('Cyberport', 'Pok Fu Lam')
            df['District'] = df['District'].str.replace('Kai Tak Ferry Terminal', 'Kowloon Bay')
            df['District'] = df['District'].str.replace('Tsueng Kwan O', 'Tseung Kwan O')
            df['District'] = df['District'].str.replace('Heng Fa Chuen', 'Chai Wan')
            df['District'] = df['District'].str.replace('Soho', 'Central')
            
            # adjust district by tesla web english address
            df['District'] = np.where(df['EnglishAddress'].str.contains('Repulse Bay'), 'Repulse Bay', df['District'])
            df['District'] = np.where(df['EnglishAddress'].str.contains('Ngau Pei Sha'), 'Sha Tin', df['District'])
            
            return df
        
        def lookup_gov_district(df):
            
            '''
            After the adjustment of tesla web district, loop up the gov district
            '''
            
            lookup_df = self.get_lookup_gov_district()

            df = pd.merge(df,
                          lookup_df,
                          how='left',
                          on='District')
            
            return df
        
        def name_format(df):
            
            name_col_list = ['EnglishName', 'ChineseName', 'EnglishAddress', 'ChineseAddress']
            
            remove_str_list = [' Supercharger', '超級充電站']
            
            for name in name_col_list:
                
                for remove_str in remove_str_list:
            
                    df[name] = df[name].str.replace(remove_str, '')
                    df[name] = df[name].str.replace("'", '')
                                
            return df
        
        def detail_format(df):
            
            df['Detail'] = df['Detail'].replace('\n','', regex=True)
            
            return df

        def final_adjustment(df):
            
            lookup_df = self.get_lookup_final_adj()
            
            # if need to adj
            if len(lookup_df) > 0:
                
                for index, row in lookup_df.iterrows():
                    
                    try:
                        con_col = row['ConditionalColumn']
                        con_val = row['ConditionalValue']
                        cho_col = row['ChoiceColumn']
                        cho_val = row['ChoiceValue']
                        
                        df[cho_col] = np.where(df[con_col] == con_val,
                                               cho_val,
                                               df[cho_col])
                    except:
                        pass  
                
            return df
        
        def append_kilowatt(df):
            
            kilowatt_df = self.get_append_kilowatt()
            
            df = pd.concat([df, kilowatt_df])
            
            df['Number'] = pd.to_numeric(df['Number'], errors='coerce', downcast='integer')
            
            return df
        
        def charger_brand(df):
            
            df['ChargerBrand'] = 'Tesla'
            
            return df
        
        def update_date(df):
            
            df['UpdateDate'] = pd.Timestamp.now()  
            
            df['UpdateDate'] = pd.to_datetime(df['UpdateDate']).dt.strftime('%Y-%m-%d %H:%M')
            
            return df

        def col_order(df):
            
            col_order_list = ['EnglishName', 'ChineseName', 
                              'EnglishAddress', 'ChineseAddress',
                              'District', 'GovDistrict', 'Region',
                              'ChargerType', 'Number', 
                              'Detail', 'Source', 'ChargerBrand', 'UpdateDate']

            df = df.reindex(columns=col_order_list)
            
            return df
        
        def fillna(df):
            
            float_cols = df.select_dtypes(include=['float64']).columns
            str_cols = df.select_dtypes(include=['object']).columns
            
            df.loc[:, float_cols] = df.loc[:, float_cols].fillna(0)
            df.loc[:, str_cols] = df.loc[:, str_cols].fillna('')
                        
            return df
            
        df = (self.df.copy()
              .pipe(exclude_record)
              .pipe(split_eng_chinese_name)
              .pipe(trim_name)
              .pipe(adjust_chinese_name)
              .pipe(charger_number_tesla_web)
              .pipe(adjustment_district)
              .pipe(grouping_by_address)
              .pipe(chinese_address_step1)
              .pipe(chinese_address_step2)
              .pipe(adj_district)
              .pipe(lookup_gov_district)
              .pipe(name_format)
              .pipe(detail_format)
              .pipe(final_adjustment)
              .pipe(append_kilowatt)
              .pipe(charger_brand)
              .pipe(update_date)
              .pipe(col_order)
              .pipe(fillna)
            )
        
        # tesla_pipeline
        return df

