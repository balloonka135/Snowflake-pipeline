import os
from google.cloud import storage
from flask import Flask
import pandas as pd
import numpy as np
import yfinance as yf
import utils

# initialize Flask application
app = Flask(__name__)


@app.route('/')
def main():
    """Triggered from a message on a Cloud Pub/Sub topic.
    
    Parameters:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # configure access to the bucket
    bucket = utils.config_bucket_conn()
    if bucket:
        # read the product list csv file from GCS bucket
        product_df = pd.read_csv('gs://{}/product_list.csv'.format(utils.GCS_BUCKET_NAME), sep='|')

        # initialize lists for dataframes containing context specific data for every product
        list_of_profile_df = []
        list_of_financial_df = []
        list_of_price_history_df = []
        list_of_divident_df = []
        list_of_earnings_df = []

        # iterate over product list and generate multiple dataframes with extracted data (profile, historical, etc)
        for index, row in product_df.iterrows():

            # extract the product ID and Yahoo ticker from the product list
            product_id = row['product_id']
            ticker = row['ticker_yahoo_finance']
            
            # access the Yahoo Finance data related to a product
            product_obj = yf.Ticker(ticker)
            
            # check if data is not empty for particular product
            if len(product_obj.info) == 1:
                continue

            # extract and save the context specific data into intermediate dataframes
            profile_df = utils.extract_profile_data(product_id, product_obj)
            print('Extracted profile data for product {}'.format(product_id))
            list_of_profile_df.append(profile_df)

            financial_df = utils.extract_financial_data(product_id, product_obj)
            print('Extracted financial data for product {}'.format(product_id))
            list_of_financial_df.append(financial_df)
            
            historical_price_df = utils.extract_historical_price_data(product_id, product_obj)
            print('Extracted historical price data for product {}'.format(product_id))
            list_of_price_history_df.append(historical_price_df)
            
            divident_df = utils.extract_divident_data(product_id, product_obj)
            print('Extracted dividents data for product {}'.format(product_id))
            list_of_divident_df.append(divident_df)
            
            earnings_df = utils.extract_earnings_data(product_id, product_obj)
            print('Extracted earnings data for product {}'.format(product_id))
            list_of_earnings_df.append(earnings_df)
            

        # merge every list of contextual dataframes into one dataframe
        merged_profile_df = pd.concat(list_of_profile_df)
        merged_profile_df.reset_index(inplace=True, drop=True)

        merged_financial_df = pd.concat(list_of_financial_df)
        merged_financial_df.reset_index(inplace=True, drop=True)

        merged_price_history_df = pd.concat(list_of_price_history_df)
        merged_price_history_df.reset_index(inplace=True)

        merged_divident_df = pd.concat(list_of_divident_df)
        merged_divident_df.reset_index(inplace=True)

        merged_earnings_df = pd.concat(list_of_earnings_df)
        merged_earnings_df.reset_index(inplace=True)
        merged_earnings_df.rename(columns={'index':'Year'}, inplace=True)

        # write the data into bucket
        utils.write_df_to_bucket(bucket, merged_profile_df, 'profile')
        utils.write_df_to_bucket(bucket, merged_financial_df, 'financial')
        utils.write_df_to_bucket(bucket, merged_price_history_df, 'price_history')
        utils.write_df_to_bucket(bucket, merged_divident_df, 'dividents')
        utils.write_df_to_bucket(bucket, merged_earnings_df, 'earnings')

        return 'The data extraction finished successfully'
    else:
        return 'Error. Bucket does not exist'


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)
