import os
import google.cloud.exceptions as gcloud_exception
from google.cloud import storage
import pandas as pd
import csv
import numpy as np
import yfinance as yf

GCS_BUCKET_NAME = 'data_bucket'


def config_bucket_conn():
    '''
    Method for configuring the connection to the GCS bucket.
    
    Returns:
    (google.cloud.storage.bucket.Bucket): Instance of the GCS bucket
    '''
    
    # configure credentials only if this code is running locally
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'./credentials.json'


    client = storage.Client()
    try:
        bucket = client.get_bucket(GCS_BUCKET_NAME)
    except gcloud_exception.NotFound:
        print('Error. Bucket does not exist')
        return None
    else:
        print('GCS {} bucket successfully configured\n'.format(GCS_BUCKET_NAME))
        return bucket


def write_df_to_bucket(bucket, df, filename):
    '''
    Method for writing the dataframe as a csv file into a GCS bucket.
    
    Parameters:
    bucket (google.cloud.storage.bucket.Bucket): configured connection to the GCS bucket
    df (pandas.core.frame.DataFrame): Dataframe whose content has to be saved to a csv file
    filename (str): Name of the file that should be written to a bucket 

    '''
    try:
        bucket.blob('{}.csv'.format(filename)).upload_from_string(df.to_csv(index=False, sep='|', quoting=csv.QUOTE_NONE), 'text/csv')
    except gcloud_exception.GoogleCloudError as err:
        print(err)
        print('Upload of data was failed')
    else:
        print('Successfully written {} to the bucket'.format(filename))


def extract_profile_data(product_id, product_obj):
    '''
    Method for extracting Profile data (description, sector, etc.).
    
    Parameters:
    product_id (str): Product ID 
    product_obj (yfinance.Ticker object): Product object containing Yahoo Finance information

    Returns:
    profile_df (pandas.core.frame.DataFrame): Dataframe with information of product Profile
    '''

    # list of keys that should not be filtered out
    info_keys = ['zip', 'sector', 'fullTimeEmployees', 'longBusinessSummary', 'city', \
                 'phone', 'country', 'website'
    ]
    # filtered out dictionary from product_obj.info
    updated_info_dict = {}
    for key in info_keys:
        if key in product_obj.info:
            updated_info_dict[key] = product_obj.info.get(key)
        else:
            updated_info_dict[key] = np.nan

    profile_df = pd.DataFrame([updated_info_dict], columns=updated_info_dict.keys())
    
    # add the product_id column to the dataframe
    profile_df['product_id'] = product_id
    return profile_df


def extract_financial_data(product_id, product_obj):
    '''
    Method for Financial indicators (market cap, EPS, P/E ratio, etc).
    
    Parameters:
    product_id (str): Product ID 
    product_obj (yfinance.Ticker object): Product object containing Yahoo Finance information

    Returns:
    finance_df (pandas.core.frame.DataFrame): Dataframe with information of product Financial indicators
    '''

    # list of keys that should not be filtered out
    info_keys = ['forwardEps', 'trailingEps', 'marketCap', 'trailingPE', 'forwardPE']

    # filtered out dictionary from product_obj.info
    updated_info_dict = {}
    for key in info_keys:
        if key in product_obj.info:
            updated_info_dict[key] = product_obj.info.get(key)
        else:
            updated_info_dict[key] = np.nan

    finance_df = pd.DataFrame([updated_info_dict], columns=updated_info_dict.keys())
    
    # add the product_id column to the dataframe
    finance_df['product_id'] = product_id
    return finance_df


def extract_historical_price_data(product_id, product_obj):
    '''
    Method for extracting price data for the past 5 years (open, close, low, high, volume).
    
    Parameters:
    product_id (str): Product ID 
    product_obj (yfinance.Ticker object): Product object containing Yahoo Finance information

    Returns:
    (pandas.core.frame.DataFrame): Dataframe with information of product price data for the past 5 years
    '''
    
    # extract  historical data using product_obj.history() method
    product_historical_df = product_obj.history(period='5y')
    
    # reduce dataframe to include only relevant columns
    product_historical_df = product_historical_df[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    # add the product_id column to the dataframe
    product_historical_df['product_id'] = product_id
    
    return product_historical_df


def extract_divident_data(product_id, product_obj):
    '''
    Method for extracting Dividend related data (e.g. dividend date, yield).
    
    Parameters:
    product_id (str): Product ID 
    product_obj (yfinance.Ticker object): Product object containing Yahoo Finance information

    Returns:
    (pandas.core.frame.DataFrame): Dataframe with information of product yielded dividents
    '''
    
    divid_df = pd.DataFrame(product_obj.dividends)
    divid_df['product_id'] = product_id
    
    return divid_df


def extract_earnings_data(product_id, product_obj):
    '''
    Method for extracting Earnings related data (e.g. earnings date, revenue).
    
    Parameters:
    product_id (str): Product ID 
    product_obj (yfinance.Ticker object): Product object containing Yahoo Finance information

    Returns:
    (pandas.core.frame.DataFrame): Dataframe with information of product revenue and earnings
    '''
    earn_df = pd.DataFrame(product_obj.earnings)
    
    # check if the revenue and earnings data is present for product object
    if 'Earnings' and 'Revenue' in earn_df:
        earn_df = earn_df[['Revenue', 'Earnings']]
    else:
        earn_df['Revenue'] = np.nan
        earn_df['Earnings'] = np.nan
    earn_df['product_id'] = product_id
    
    return earn_df
