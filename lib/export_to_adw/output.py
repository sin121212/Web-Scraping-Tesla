# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 09:47:17 2022

@author: kitsin
"""

import pandas as pd
import numpy as np

from tqdm import tqdm
import time
from sqlalchemy import inspect

#%%

class Output():
    
    def __init__(self, df, con, schema, table_name):
        
        self.df = df
        self.con = con
        self.schema = schema
        self.table_name = table_name

        pass
    
    def check_adw_table_exist(self):
        
        '''
        table_exist: return true or false
        '''
        
        insp = inspect(self.con)
        table_exist = insp.has_table(self.table_name, schema='dbo')
        
        return table_exist
        
    def excel_to_adw(self):
        
        def read_member_excel():
            
            # set data type = object
            df = self.df
            
            print(df.info())
            
            # read_member_excel
            return df

        def to_adw(df):
            
            '''
            Check Table Exist
                if not exist, create table then upload records
                if exist, clear table then upload records
            '''
            

            
            print(f'table {self.schema}.{self.table_name} exist: {self.check_adw_table_exist()}')
            
            if self.check_adw_table_exist() == False:
                
                q = f"CREATE TABLE {self.schema}.{self.table_name} ( \
                            EnglishAddress nvarchar(255), \
                            ChineseAddress nvarchar(255), \
                            EnglishName varchar(255), \
                            ChineseName nvarchar(255), \
                            District varchar(255), \
                            GovDistrict varchar(255), \
                            Region varchar(255), \
                            Detail varchar(255), \
                            Source varchar(255), \
                            ChargerBrand varchar(255), \
                            UpdateDate datetime, \
                            ChargerType varchar(255), \
                            Number int \
                            ) "
                
                self.con.execute(q)
                print(f'Created table {self.schema}.{self.table_name}')

            if self.check_adw_table_exist() == True:
                
                q = f"DELETE FROM {self.schema}.{self.table_name} "
                self.con.execute(q)
                print('Removed old record')
                
            print('Uploading')
            def gen_sql_query():
                
                '''
                Return a list of insert sql query from subset dataframe
                '''
                
                q1 = f"INSERT INTO {self.schema}.{self.table_name} ( \
                            EnglishAddress, \
                            ChineseAddress, \
                            EnglishName, \
                            ChineseName, \
                            District, \
                            GovDistrict, \
                            Region, \
                            Detail, \
                            Source, \
                            ChargerBrand, \
                            UpdateDate, \
                            ChargerType, \
                            Number \
                            ) "
        
                # split dataframe to subset
                split_number = int(len(df) / 50) + 1
                print(f'split_number: {split_number}')
                sub_df_list = np.array_split(df, split_number)
                
                q_list = []
                
                for sub_df in sub_df_list:
                    sub_q_list = []
                    
                    for index, row in tqdm(sub_df.iterrows()):
                        sub_q_list.append(f"SELECT '{row['EnglishAddress']}', \
                                                      N'{row['ChineseAddress']}', \
                                                      N'{row['EnglishName']}',  \
                                                      N'{row['ChineseName']}', \
                                                      '{row['District']}',  \
                                                      '{row['GovDistrict']}', \
                                                      '{row['Region']}', \
                                                      '{row['Detail']}', \
                                                      '{row['Source']}', \
                                                      '{row['ChargerBrand']}', \
                                                      '{row['UpdateDate']}', \
                                                      '{row['ChargerType']}',  \
                                                      {row['Number']}  \
                                                      ")
                    
                    sub_q = ' UNION '.join(sub_q_list)
                    
                    sub_q = q1 + sub_q
                    
                    q_list.append(sub_q)
                
                return q_list
            
            insert_q_list = gen_sql_query()
            
            start_time = time.time()
            for sql_i in tqdm(insert_q_list):
                self.con.execute(sql_i)
            print(f"{time.time() - start_time} sec")
            
            # to_adw
            return df
        
        df = (read_member_excel()
              .pipe(to_adw)
              )
    
        # excel_to_adw
        return df