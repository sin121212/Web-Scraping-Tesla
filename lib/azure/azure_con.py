# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 14:42:03 2022

@author: kitsin
"""

from sqlalchemy.engine import URL
from sqlalchemy import create_engine

import pyodbc

import pandas as pd

#%%

class AzureDataWarehouse():
    
    def __init__(self, driver, username, password, host, database):
    
        self.driver = driver
        self.username = username
        self.password = password
        self.host = host
        self.database = database

        self.db_con = self.azure_db_engine()
        
        pass

    def azure_db_engine(self):
        
        # for read sql
        
        connection_url = URL.create('mssql+pyodbc',
                                    username=self.username,
                                    password=self.password,
                                    host=self.host,
                                    database=self.database,
                                    query={
                                        'driver': self.driver,
                                        'autocommit': 'True',
                                        'fast_executemany ': 'True'
                                        },
                                    )
    
        engine = create_engine(connection_url)
        
        return engine
    
    def azure_db_cursor(self):
        
        # for to sql
        
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                              server=self.host,
                              database=self.database,
                              uid=self.username, 
                              pwd=self.password)
        
        return conn

