# Yahoo Finance automatic data pipeline

The following repository contains the essential scripts for creating the automatic data pipeline for extracting the market data from Yahoo Finance and ingesting it into Snowflake. Specifically:
- `main.py`
> contains the code to start the Flask application that performs data extraction from Yahoo Finance and loads the extracted data into the GCS bucket
- `utils.py`
> contains methods for configuring the GCS bucket connection, extracting data from the Yahoo Finance and writing it to the GCS bucket as csv files
- `tests.py`
> contains unit tests for verifying the correct execution of the methods from utils.py and overall app functionality
- `app.yaml`
> congiguration file for deploying the application into Google App Engine
- `cron.yaml`
> configuration file for defining scheduled cron job for the application
- `sf_pipeline.sql`
> Snowflake data pipeline script


## Running the application locally

### Prerequisites
- Create a Service Account for the Google Cloud project and generate credentials.json file following the [tutorial](https://cloud.google.com/docs/authentication/getting-started#cloud-console).
- Create a GCS bucket and assign its name to the ```GCS_BUCKET_NAME``` in utils.py.
- Install Google SDK following the [tutorial](https://cloud.google.com/sdk/docs/install).
- Install necessary dependencies within virtual environment by running the command from the terminal
```pip install -r requirements.txt```.

1. Uncomment the `GOOGLE_APPLICATION_CREDENTIALS` environment variable export in utils.py (line 21).
2. Run `main.py`


## Running tests
- Run `python -m pytest tests.py` in the terminal within the directory root

## Deploying application to the App Engine
1. Create (if don't exist) or edit `app.yaml` and `cron.yaml` file
2. Comment the `GOOGLE_APPLICATION_CREDENTIALS` environment variable export in utils.py (line 21), as it is used only for dev environment.
3 Run `gcloud app deploy` from the terminal within the directory root
4. Run `gcloud app deploy cron.yaml` from the terminal to configure a cron job schedule

## General flow overview:
1. Deployed to App Engine Flask application using the Python `yfinance` library extracts the market data for every product ID within the `product_list.csv` file.
2. Application is scheduled to be executed every 24 hours and generate new files containing extracted data to the GCS bucket.
3. Snowflake pipeline contains configured integration with the GCS bucket and loads the csv files from the bucket into the external stage.
4. Snowflake pipeline contains scheduled tasks for truncating data from the defined tables on a daily basis and loading the new data from the external stage into the tables.
5. The data warehouse has a star schema, where `PRODUCT_FACT_TABLE` is a fact table containing list of product ID and all related information, and `PRODUCT_PROFILE`, `FINANCIAL_INDICATORS`, `PRICE_HISTORY`, `DIVIDENTS` and `EARNINGS` dimension tables containing market data for every corresponding product ID.
