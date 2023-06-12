#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 15:26:22 2023

@author: aditidadariya
"""

import utility as util
import os


# ========================== CREATE YAML FILE ========================================

# Created a new empty yaml file to store the global parameters. 
#util.CreateYAMLFle()           # Commeted this line as the yaml file has been created initially and do not need to be created again

# Load config.yaml file and store it as a dictionary object
param = util.LoadYAMLFile("config.yaml")


# ======================= READ CSV FILE USING PANDAS read_csv ========================

# Read the file using PANDAS read_csv method from local location and store it in a dataframe
df_data = util.PD_read_csv(param['filename'], param['delimiter'], param['columns'])

# Write data in text file
util.WriteInTextFile("The dataset has Rows {} and Columns {} ".format(df_data.shape[0], df_data.shape[1]))

# Add a new line here
util.Newline()

# Print the dataframe
#print(df_data)

# Add a new line here
util.Newline()

# ======================== READ CSV FILE USING DASK read_csv =========================

# Read the file using DASK read_csv method from local location and store it in a dataframe
ddf_data = util.Dask_read_csv(param['filename'], param['delimiter'], param['columns'])

# Add a new line here
util.Newline()

# Print the dataframe
#print(ddf_data.compute())

# Add a new line here
util.Newline()

# ========================= READ CSV FILE USING DATATABLE read_csv =========================

# Add a new line here
util.Newline()    

dtdf_data = util.DT_read_csv(param['filename'], param['delimiter'], param['columns'])

# Add a new line here
util.Newline()

#print(dtdf_data.head())

# ========================= READ CSV FILE USING MODIN read_csv =========================


# Add a new line here
util.Newline()

# Read the file using Modin_read_csv method from local location and store it in a dataframe
mdf_data = util.Modin_read_csv(param['filename'],param['delimiter'], param['columns'])

# Add a new line here
util.Newline()

#print(mdf_data.head())

# ============================== FIND THE FILE SIZE ===================================

# Get the file size in bytes
file_size = os.path.getsize(param['filename'])
# Convert bytes to gigabytes
file_size_gb = file_size / (1024 * 1024 * 1024)

# ========================= WRITE DETAILS IN TEXT FILE ================================

util.WriteInTextFile("The size of the file is {} GB".format(file_size_gb))

# =============================== FIND MISSING VALUES =================================

# Add a new line here
util.Newline()

# Printing the missing values (NaN)
print(df_data.isnull().sum())

# Add a new line here
util.Newline()

# Output:
#    Timestamp             0
#    From Bank             0
#    Account               0
#    To Bank               0
#    Account.1             0
#    Amount Received       0
#    Receiving Currency    0
#    Amount Paid           0
#    Payment Currency      0
#    Payment Format        0
#    Is Laundering         0
#    dtype: int64

# ========================= REMOVE SPACE FROM COLUMN NAME =============================

# Calling RemoveWhiteSpace function to remove space from column name 
df_data = util.RemoveWhiteSpace(df_data)
print(df_data.columns)

# Add a new line here
util.Newline()

#Output:
# Index(['Timestamp', 'FromBank', 'Account', 'ToBank', 'Account.1',
#       'AmountReceived', 'ReceivingCurrency', 'AmountPaid', 'PaymentCurrency',
#       'PaymentFormat', 'IsLaundering'],
#      dtype='object')    

# ===================== REMOVE SPECIAL CHARACTER FROM DATASET =========================

# Calling RemoveSpecialChar function to remove the special characters
df_data = util.RemoveSpecialChar(df_data)

print(df_data.head())

#Output:
#          Timestamp  FromBank  ... PaymentFormat  IsLaundering
#0  2022/09/01 00:15        20  ...  Reinvestment             0
#1  2022/09/01 00:18      3196  ...  Reinvestment             0
#2  2022/09/01 00:23      1208  ...  Reinvestment             0
#3  2022/09/01 00:19      3203  ...  Reinvestment             0
#4  2022/09/01 00:27        20  ...  Reinvestment             0

#[5 rows x 11 columns]    
