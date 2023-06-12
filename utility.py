#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 16:03:37 2023

@author: aditidadariya
"""


import yaml
import os
import pandas as pd
from datetime import datetime

from pandas import read_csv
import dask.dataframe as dd
import datatable as dtl
import ray
import ray.data as rd
import modin.pandas as mpd
from pyarrow import csv


#==============================================================================

# Newline gives a new line
def Newline():
    print("\r\n")

# CreateYAMLFle function is defined to create a new yaml file
def CreateYAMLFle():
    # Define the data to hold empty value
    data = {}
    # Specify the file path and name for the YAML file
    file_path = 'config.yaml'
    
    # Write the data to the YAML file
    with open(file_path, 'w') as file:
        yaml.dump(data, file)
    
    print(f"YAML file '{file_path}' has been created.")

# LoadYAMLFile function is defined to read the parameters defined in config.yaml file
def LoadYAMLFile(yamlfilename):
    # Specify the file path and name of the YAML file
    file_path = yamlfilename
    
    # Load the contents of the YAML file
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    
    return data

# RemoveWhiteSpace finction is defined to remove the white spaces from column name
def RemoveWhiteSpace(df):
    # Remove white spaces from column names
    df.rename(columns=lambda x: x.replace(" ", ""), inplace=True)
    return df

# RemoveSpecialChar function is defined to remove the special character
def RemoveSpecialChar(df):
    pattern = r'[^\w\s]'

    # Iterate over each column in the DataFrame
    for column in df.columns:
        # Apply pattern matching to identify rows with special characters
        df[column].replace(pattern, '', regex=True)
        
    return df

# WriteInTextFile function is defined to add content in text file
def WriteInTextFile(stmt):
    # Open the file in write mode
    with open('textfile.txt', 'a') as file:
        # Write the data to the file
        file.write(stmt)
        file.write('\n')

# PD_read_csv function reads the dataset csv file using Pandas and store it in a dataframe
def PD_read_csv(filename, delimiter, columnnames):
    # absolute_path and location_of_file defined to locate the file
    absolute_path = os.path.dirname(__file__)
    location_of_file = os.path.join(absolute_path, filename)
    
    # Records the Start time
    Start_datetime = datetime.now()
        
    # Read the data file from specified location using Pandas
    df = pd.read_csv(location_of_file, delimiter=delimiter)
    
    # Record the End time
    End_datetime = datetime.now()
    
    # Calculate the time Pandas took to read the file
    dt = End_datetime - Start_datetime
    #print("Total time taken for Pandas to read the file is: {}".format(dt))
    WriteInTextFile("Total time taken for Pandas to read the file is: {}".format(dt))
    # Compare DataFrame column names with the columns fetched from config.yaml file
    matching_columns = [col for col in df.columns if col in columnnames]
    
    if len(matching_columns) == len(df.columns) and list(matching_columns) == list(df.columns):
        return df
    else:
        return print("Columns are not same as specified in configurations")
    

# Dask_read_csv function reads the dataset csv file using Dask and store it in a dataframe
def Dask_read_csv(filename, delimiter, columnnames):
    # Records the Start time
    Start_datetime = datetime.now()
    
    # Read the data file using Dask
    ddf = dd.read_csv(filename, delimiter=delimiter, assume_missing=True)
    
    # Record the End time
    End_datetime = datetime.now()
    
    # Calculate the time Pandas took to read the file
    dt = End_datetime - Start_datetime
    #print("Total time taken for Dask to read the file is: {}".format(dt))
    WriteInTextFile("Total time taken for Dask to read the file is: {}".format(dt))
    # Compare DataFrame column names with the columns fetched from config.yaml file
    matching_columns = [col for col in ddf.columns if col in columnnames]
    
    if len(matching_columns) == len(ddf.columns) and list(matching_columns) == list(ddf.columns):
        return ddf
    else:
        return print("Columns are not same as specified in configurations")
    
def DT_read_csv(filename, delimiter, columnnames):
    # Records the Start time
    Start_datetime = datetime.now()
    
    # Read the data file from specified location using Pandas
    dtdf = dtl.fread(filename, sep=delimiter)
    
    # Record the End time
    End_datetime = datetime.now()
    
    # Calculate the time Pandas took to read the file
    dt = End_datetime - Start_datetime
    #print("Total time taken for Datatable to read the file is: {}".format(dt))
    WriteInTextFile("Total time taken for Datatable to read the file is: {}".format(dt))
    # Convert datatable.frame to pandas dataframe
    dtldf = dtdf.to_pandas()
    
    # Compare DataFrame column names with the columns fetched from config.yaml file
    matching_columns = [col for col in dtldf.columns if col in columnnames]
    
    if len(matching_columns) == len(dtldf.columns) and list(matching_columns) == list(dtldf.columns):
        return dtldf
    else:
        return print("Columns are not same as specified in configurations")


def Modin_read_csv(filename, delimiter, columnnames):
    ray.init()
    # Records the Start time
    Start_datetime = datetime.now()
    
    # Read the data file using Modin
    mdf = mpd.read_csv(filename, delimiter=delimiter)
    
    # Record the End time
    End_datetime = datetime.now()
    
    # Calculate the time Pandas took to read the file
    dt = End_datetime - Start_datetime
    #print("Total time taken for Modin to read the file is: {}".format(dt))
    WriteInTextFile("Total time taken for Modin to read the file is: {}".format(dt))
    # Compare DataFrame column names with the columns fetched from config.yaml file
    matching_columns = [col for col in mdf.columns if col in columnnames]
    
    if len(matching_columns) == len(mdf.columns) and list(matching_columns) == list(mdf.columns):
        return mdf
    else:
        return print("Columns are not same as specified in configurations")
    
    ray.shutdown()
    
 