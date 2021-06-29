use role INAZARCHUK;
use warehouse INAZARCHUK_WH;

create schema INAZARCHUK.YAHOO_DATA;

show schemas;


-- Create a Cloud Storage Integration in Snowflake
create storage integration gcs_int
  type = external_stage
  storage_provider = gcs
  enabled = true
  storage_allowed_locations = ('gcs://data_bucket/');
  
grant usage on integration gcs_int to role INAZARCHUK;

-- Retrieve the Cloud Storage Service Account for your Snowflake Account
desc storage integration gcs_int;

create or replace file format my_csv_format
  type = 'CSV'
  field_delimiter = '|'
  skip_header = 1
  skip_blank_lines = true
  error_on_column_count_mismatch = true
  null_if=('NULL','',' ','NULL','NULL','//N') 
;
 
-- Creating an external stage
grant create stage on schema INAZARCHUK.YAHOO_DATA to role INAZARCHUK;

grant usage on integration gcs_int to role INAZARCHUK;

use schema INAZARCHUK.YAHOO_DATA;

create or replace stage my_gcs_stage
  url = 'gcs://data_bucket/'
  storage_integration = gcs_int
  file_format = my_csv_format;
  
  
-- Loading and Tranforming the data
list @my_gcs_stage;


-- Query the staged data file
select t.$1, t.$2, t.$3, t.$4 from @my_gcs_stage/product_list.csv t; 

select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9 from @my_gcs_stage/profile.csv t;

select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6 from @my_gcs_stage/financial.csv t;

select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7 from @my_gcs_stage/price_history.csv t;

select t.$1, t.$2, t.$3 from @my_gcs_stage/dividents.csv t;

-- use only 1-4 columns
select t.$1, t.$2, t.$3, t.$4 from @my_gcs_stage/earnings.csv t;


-- Create the target tables
create or replace table PRODUCT_FACT_TABLE (
  PRODUCT_ID varchar(15) not null primary key,
  PRODUCT_NAME varchar(50) not null,
  ISIN varchar(25) not null,
  TICKER_YAHOO_FINANCE varchar(10) not null
);

create or replace table PRODUCT_PROFILE (
  PRODUCT_ID varchar(15) not null primary key,
  SECTOR varchar(50),
  DESCRIPTION varchar(5000000) not null,
  N_EMPLOYEES number(38, 0),
  ZIP varchar(15),
  CITY varchar(30),
  COUNTRY varchar(45),
  PHONE varchar(30),
  WEBSITE varchar(40)
);


create or replace table FINANCIAL_INDICATORS (
  FI_SK integer not null autoincrement start 1 increment 1 primary key,
  PRODUCT_ID varchar(15) not null,
  MARKET_CAP number(38, 0),
  FORWARD_EPS number(30, 3),
  TRAILING_EPS number(30, 3),
  TRAILING_PE number(30, 6),
  FORWARD_PE number(30, 6)
);


create or replace table PRICE_HISTORY (
  PH_SK integer not null autoincrement start 1 increment 1 primary key,
  PRODUCT_ID varchar(15) not null,
  PRICE_DATE date,
  OPEN_PRICE number(38, 15),
  CLOSE_PRICE number(38, 15),
  LOW number(38, 15),
  HIGH number(38, 15),
  VOLUME number(38, 1)
);


create or replace table DIVIDENTS (
  DIVIDENT_SK integer not null autoincrement start 1 increment 1 primary key,
  PRODUCT_ID varchar(15) not null,
  DIVIDENT_DATE date,
  YIELD number(30, 5) not null
);

create or replace table EARNINGS (
  ERN_SK integer not null autoincrement start 1 increment 1 primary key,
  PRODUCT_ID varchar(15) not null,
  EARNINGS_YEAR integer,
  REVENUE number(38, 1) not null,
  EARNIGS number(38, 1) not null
);


-- Truncate tables if exist before loading new data
create task TRUNCATE_PROFILE_DATA_TASK
  warehouse = INAZARCHUK_WH
  schedule = 'using cron 0 5 * * * UTC'
as
truncate table if exists PRODUCT_PROFILE;

create task TRUNCATE_FINANCIAL_DATA_TASK
  warehouse = INAZARCHUK_WH
  schedule = 'using cron 0 5 * * * UTC'
as
truncate table if exists FINANCIAL_INDICATORS;

create task TRUNCATE_PRICE_HISTORY_DATA_TASK
  warehouse = INAZARCHUK_WH
  schedule = 'using cron 0 5 * * * UTC'
as
truncate table if exists PRICE_HISTORY;

create task TRUNCATE_DIVIDENTS_DATA_TASK
  warehouse = INAZARCHUK_WH
  schedule = 'using cron 0 5 * * * UTC'
as
truncate table if exists DIVIDENTS;

create task TRUNCATE_EARNINGS_DATA_TASK
  warehouse = INAZARCHUK_WH
  schedule = 'using cron 0 5 * * * UTC'
as
truncate table if exists EARNINGS;

-- Resume tasks
alter task LOAD_PROFILE_DATA_TASK resume;
alter task LOAD_FINANCIAL_DATA_TASK resume;
alter task LOAD_PRICE_HISTORY_DATA_TASK resume;
alter task LOAD_DIVIDENTS_DATA_TASK resume;
alter task LOAD_EARNINGS_DATA_TASK resume;

alter task TRUNCATE_PROFILE_DATA_TASK resume;
alter task TRUNCATE_PRICE_HISTORY_DATA_TASK resume;
alter task TRUNCATE_FINANCIAL_DATA_TASK resume;
alter task TRUNCATE_EARNINGS_DATA_TASK resume;
alter task TRUNCATE_DIVIDENTS_DATA_TASK resume;

show tasks;


-- Convert the staged CSV column data to the specified data types and load it into the destination tables (runs on a daily basis)

-- Product data (only loaded once)
copy into PRODUCT_FACT_TABLE(PRODUCT_ID, PRODUCT_NAME, ISIN, TICKER_YAHOO_FINANCE)
  from (
  select distinct to_varchar(t.$1), to_varchar(t.$2), to_varchar(t.$3), to_varchar(t.$4) 
  from @my_gcs_stage t
  )
  pattern='.*product_list.*.csv'
  file_format = (format_name = my_csv_format);


-- Profile data
create task LOAD_PROFILE_DATA_TASK
  warehouse = INAZARCHUK_WH
  after TRUNCATE_PROFILE_DATA_TASK
as
copy into PRODUCT_PROFILE(PRODUCT_ID, SECTOR, DESCRIPTION, N_EMPLOYEES, ZIP, CITY, COUNTRY, PHONE, WEBSITE)
  from (
  select distinct to_varchar(t.$9), to_varchar(t.$2), to_varchar(t.$4), to_number(t.$3), to_varchar(t.$1), to_varchar(t.$5), to_varchar(t.$7), to_varchar(t.$6), to_varchar(t.$8)
  from @my_gcs_stage t
  )
  pattern='.*profile.*.csv'
  file_format = (format_name = my_csv_format);


-- Financial data
create task LOAD_FINANCIAL_DATA_TASK
  warehouse = INAZARCHUK_WH
  after TRUNCATE_FINANCIAL_DATA_TASK
as
copy into FINANCIAL_INDICATORS(PRODUCT_ID, MARKET_CAP, FORWARD_EPS, TRAILING_EPS, TRAILING_PE, FORWARD_PE)
  from (
  select distinct to_varchar(t.$6), to_number(t.$3), to_number(t.$1, 30, 3), to_number(t.$2, 30, 3), to_number(t.$4, 30, 6), to_number(t.$5, 30, 6)
  from @my_gcs_stage t
  )
  pattern='.*financial.*.csv'
  file_format = (format_name = my_csv_format);



-- Price history data
create task LOAD_PRICE_HISTORY_DATA_TASK
  warehouse = INAZARCHUK_WH
  after TRUNCATE_PRICE_HISTORY_DATA_TASK
as
copy into PRICE_HISTORY(PRODUCT_ID, PRICE_DATE, OPEN_PRICE, CLOSE_PRICE, LOW, HIGH, VOLUME)
  from (
  select distinct to_varchar(t.$7), to_date(t.$1), to_number(t.$2, 38, 15), to_number(t.$5, 38, 15), to_number(t.$4, 28, 15), to_number(t.$3, 38, 15), to_number(t.$6, 38, 1)
  from @my_gcs_stage t
  )
  pattern='.*price_history.*.csv'
  file_format = (format_name = my_csv_format);
  
 
-- Dividents data
create task LOAD_DIVIDENTS_DATA_TASK
  warehouse = INAZARCHUK_WH
  after TRUNCATE_DIVIDENTS_DATA_TASK
as
copy into DIVIDENTS(PRODUCT_ID, DIVIDENT_DATE, YIELD)
  from (
  select distinct to_varchar(t.$3), to_date(t.$1), to_number(t.$2, 30, 5)
  from @my_gcs_stage t
  )
  pattern='.*dividents.*.csv'
  file_format = (format_name = my_csv_format);
   
 
-- Earnings data
create task LOAD_EARNINGS_DATA_TASK
  warehouse = INAZARCHUK_WH
  after TRUNCATE_EARNINGS_DATA_TASK
as
copy into EARNINGS(PRODUCT_ID, EARNINGS_YEAR, REVENUE, EARNIGS)
  from (
  select distinct to_varchar(t.$4), to_number(t.$1), to_number(t.$2, 38, 1), to_number(t.$3, 38, 1)
  from @my_gcs_stage t
  )
  pattern='.*earnings.*.csv'
  file_format = (format_name = my_csv_format);


-- Query the target tables
select * from PRODUCT_FACT_TABLE;

select * from PRODUCT_PROFILE;

select * from FINANCIAL_INDICATORS;

select * from PRICE_HISTORY order by PRICE_DATE DESC;

select * from DIVIDENTS;

select * from EARNINGS;
