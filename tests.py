import pandas as pd
import numpy as np
import yfinance as yf
import utils

def test_config_bucket_conn():
    """
    Method for testing that utils.config_bucket_conn()
    initializes connection with the GCS bucket.
    """
    assert utils.config_bucket_conn()


def test_extracted_data_bucket_upload():
    """
    Method for testing that utils.write_df_to_bucket()
    writes a dataframe to the bucket.
    """
    bucket = utils.config_bucket_conn()
    test_df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                   columns=['a', 'b', 'c'])
    utils.write_df_to_bucket(bucket, test_df, 'pytest_df')
    extracted_test_df = pd.read_csv('gs://{}/pytest_df.csv'.format(utils.GCS_BUCKET_NAME), sep='|')
    assert test_df.equals(extracted_test_df)


def test_profile_data_extracted_non_empty():
    """
    Method for testing that utils.extract_profile_data()
    returns a non-empty dataframe.
    """
    test_product_object = yf.Ticker('AAPL')
    test_product_id = 'sb26513'
    profile_df = utils.extract_profile_data(test_product_id, test_product_object)
    assert not profile_df.empty


def test_financial_data_extracted_non_empty():
    """
    Method for testing that utils.extract_financial_data()
    returns a non-empty dataframe.
    """
    test_product_object = yf.Ticker('AAPL')
    test_product_id = 'sb26513'
    profile_df = utils.extract_financial_data(test_product_id, test_product_object)
    assert not profile_df.empty


def test_price_history_data_extracted_non_empty():
    """
    Method for testing that utils.extract_historical_price_data()
    returns a non-empty dataframe.
    """
    test_product_object = yf.Ticker('AAPL')
    test_product_id = 'sb26513'
    profile_df = utils.extract_historical_price_data(test_product_id, test_product_object)
    assert not profile_df.empty


def test_dividents_data_extracted_non_empty():
    """
    Method for testing that utils.extract_divident_data()
    returns a non-empty dataframe.
    """
    test_product_object = yf.Ticker('AAPL')
    test_product_id = 'sb26513'
    profile_df = utils.extract_divident_data(test_product_id, test_product_object)
    assert not profile_df.empty


def test_earnings_data_extracted_non_empty():
    """
    Method for testing that utils.extract_earnings_data()
    returns a non-empty dataframe.
    """
    test_product_object = yf.Ticker('AAPL')
    test_product_id = 'sb26513'
    profile_df = utils.extract_earnings_data(test_product_id, test_product_object)
    assert not profile_df.empty


def test_profile_data_contains_all_product_ids():
    """
    Method for testing that extracted profile data 
    contains all product IDs.
    """
    product_df = pd.read_csv('product_list.csv', sep='|')
    # remove product with ID sb26985 from test as Yahoo Finance does not contain data for this ticker
    product_df = product_df[product_df.product_id != 'sb26985']

    list_of_profile_df = []

    for index, row in product_df.iterrows():
        product_id = row['product_id']
        ticker = row['ticker_yahoo_finance']
            
        product_obj = yf.Ticker(ticker)

        if len(product_obj.info) == 1:
            continue
        
        profile_df = utils.extract_profile_data(product_id, product_obj)
        list_of_profile_df.append(profile_df)

    merged_profile_df = pd.concat(list_of_profile_df)
    assert (product_df.product_id.values == merged_profile_df.product_id.values).all()
    
        

def test_financial_data_contains_all_product_ids():
    """
    Method for testing that extracted financial data 
    contains all product IDs.
    """
    product_df = pd.read_csv('product_list.csv', sep='|')
    # remove product with ID sb26985 from test as Yahoo Finance does not contain data for this ticker
    product_df = product_df[product_df.product_id != 'sb26985']

    list_of_financial_df = []

    for index, row in product_df.iterrows():
        product_id = row['product_id']
        ticker = row['ticker_yahoo_finance']
            
        product_obj = yf.Ticker(ticker)

        if len(product_obj.info) == 1:
            continue
        
        financial_df = utils.extract_financial_data(product_id, product_obj)
        list_of_financial_df.append(financial_df)

    merged_financial_df = pd.concat(list_of_financial_df)
    assert (product_df.product_id.values == merged_financial_df.product_id.values).all()


def test_price_history_data_contains_all_product_ids():
    """
    Method for testing that extracted price history data 
    contains all product IDs.
    """
    product_df = pd.read_csv('product_list.csv', sep='|')
    # remove product with ID sb26985 from test as Yahoo Finance does not contain data for this ticker
    product_df = product_df[product_df.product_id != 'sb26985']

    list_of_price_history_df = []

    for index, row in product_df.iterrows():
        product_id = row['product_id']
        ticker = row['ticker_yahoo_finance']
            
        product_obj = yf.Ticker(ticker)

        if len(product_obj.info) == 1:
            continue
        
        price_history_df = utils.extract_historical_price_data(product_id, product_obj)
        list_of_price_history_df.append(price_history_df)

    merged_price_history_df = pd.concat(list_of_price_history_df)
    assert set(product_df.product_id.values) == set(merged_price_history_df.product_id.values)


def test_dividents_data_contains_all_product_ids():
    """
    Method for testing that extracted dividents data 
    contains all product IDs.

    Expected result: test fails, Yahoo Finance provided dividends data only for 178 products out of 250
    """
    product_df = pd.read_csv('product_list.csv', sep='|')
    # remove product with ID sb26985 from test as Yahoo Finance does not contain data for this ticker
    product_df = product_df[product_df.product_id != 'sb26985']

    list_of_dividents_df = []

    for index, row in product_df.iterrows():
        product_id = row['product_id']
        ticker = row['ticker_yahoo_finance']
            
        product_obj = yf.Ticker(ticker)

        if len(product_obj.info) == 1:
            continue
        
        dividents_df = utils.extract_divident_data(product_id, product_obj)
        list_of_dividents_df.append(dividents_df)

    merged_dividents_df = pd.concat(list_of_dividents_df)
    assert set(product_df.product_id.values) == set(merged_dividents_df.product_id.values)


def test_earnings_data_contains_all_product_ids():
    """
    Method for testing that extracted earnings data 
    contains all product IDs.

    Expected result: test fails, Yahoo Finance provided earnings data only for 238 products out of 250
    """
    product_df = pd.read_csv('product_list.csv', sep='|')
    # remove product with ID sb26985 from test as Yahoo Finance does not contain data for this ticker
    product_df = product_df[product_df.product_id != 'sb26985']

    list_of_earnings_df = []

    for index, row in product_df.iterrows():
        product_id = row['product_id']
        ticker = row['ticker_yahoo_finance']
            
        product_obj = yf.Ticker(ticker)

        if len(product_obj.info) == 1:
            continue
        
        earnings_df = utils.extract_earnings_data(product_id, product_obj)
        list_of_earnings_df.append(earnings_df)

    merged_earnings_df = pd.concat(list_of_earnings_df)
    assert set(product_df.product_id.values) == set(merged_earnings_df.product_id.values)