#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, text
from time import time
import calendar
from prefect import flow, task
from datetime import datetime
import os

@task()
def extract(color:str, year:int, month:int) -> pd.DataFrame:
    """Creates a url from colour, year and month parameters.
    Extracts Dataframe from URL.
    Performs premilinary data cleaning.
    """

    #setting valid arguments
    current_year=int(datetime.now().strftime("%Y"))
    valid_colors=["green", "yellow"]
    valid_years=list(range(2019, current_year))
    valid_months=list(range(1,13)) 

    if color not in valid_colors:
        raise ValueError("Colour must be yellow or green")
    elif year not in valid_years:
        raise ValueError(f"Year must be between 2019 and {current_year}") 
    elif month not in valid_months:
        raise ValueError("Month is out of range")
    else:
        parquet_url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet"
        try:
            data=pd.read_parquet(parquet_url)
        except:
            print("Problem with URL.")

    #changes all column headers to lowercase
    data.columns=[i.lower() for i in data.columns]

    return data


@task()
def transform_data(data:pd.DataFrame,year:int, month:int) -> pd.DataFrame:
    """Performs more cleaning and ensures more data consistency.
    Remove rows containing no passengers.
    Rename Columns.
    Removes irrelevant years.
    """
    #removes rows with no passengers
    data=data[data['passenger_count']!=0]

    #renaming columns
    try:
        #for green datasets
        data.rename(columns={"lpep_pickup_datetime":"pickup_datetime","lpep_dropoff_datetime":"dropoff_datetime"}, inplace=True)
    except:
        pass
    try:
        #for yellow datasets
        data.rename(columns={"tpep_pickup_datetime":"pickup_datetime","tpep_dropoff_datetime":"dropoff_datetime"}, inplace=True)
    except:
        pass

    print(data.dtypes)

    initial_size =len(data)
    #Ensures data integrity. Eliminates occurence of incorrect timestamp in dropoff column
    data["year"]=data["dropoff_datetime"].dt.year
    data=data[data["year"].isin([year])]
    #months
    data["month"]=data["dropoff_datetime"].dt.month
    data=data[data["month"].isin([month])]
    data.drop(columns=["year", "month"], inplace=True)

    #Ensures data integrity. Eliminates occurence of incorrect timestamp in pickup column
    data["year"]=data["pickup_datetime"].dt.year
    data=data[data["year"].isin([year])]
    #months
    data["month"]=data["pickup_datetime"].dt.month
    data=data[data["month"].isin([month])]
    data.drop(columns=["year", "month"], inplace=True)
    
    removed_rows=initial_size-len(data)
    print(f"Total rows removed = {removed_rows}")

    return data

@task()
def pg_connection():
    """Sets up connection to Postgres Database."""
    #Postgres Credentials
    user=os.environ["POSTGRES_USER"]
    pwd=os.environ["POSTGRES_PASSWORD"]
    host=os.environ["POSTGRES_HOST"]
    db=os.environ["POSTGRES_DB"]

    #Creates connection
    engine=create_engine(f'postgresql://{user}:{pwd}@{host}:{5432}/{db}')
    
    return engine

@flow()
def ingest_data(color:str, year:int, month:int) -> None:
    """Creates Connection to Database.
    Skips extraction and transform of data if data exists in table.
    Only adds data to the table when the specified months or years are not present in the database.
    Closes database connection afterwards.
    """
    try:
        engine=pg_connection()
        conn=engine.connect()
        print('Connection to postgres successful...')
    except:
        print("Issue with postgres connection")
    
    table_name=f"{color}_{year}_tripdata"
    

    #checks for tables in database
    query=conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"))
    table_in_db=[row for row in query]
    #checks if table exists before extracting and transforming data. Minimizes compute power.
    if (table_name,) not in table_in_db:
            raw_data=extract(color,year,month)             
            data=transform_data(raw_data,year,month)
            start_time=time()
            print(f"Creating table for dataset.")
            print(f"Starting ingestion to postgres for {calendar.month_name[month]}, {year}...")
            data.to_sql(name=table_name,con=engine,index=False, chunksize=25000)
            print('Total time of ingestion: {time} seconds'.format(time=time()-start_time))
            print(f"Data ingestion for {calendar.month_name[month]}, {year} successful")
    else:
        try:
            print(f"{table_name} already exists")
            print("Checking if month exists in table....")
            #Returns list containing distinct months in dataset in the form (month,)
            result=conn.execute(text(f"SELECT DISTINCT EXTRACT(MONTH FROM PICKUP_DATETIME) FROM {table_name}"))
            months_in_db=[row for row in result]
            #Only appends data if the specified data month doesn't exist in the table
            if (float(month),) not in months_in_db:
                print(f"Inserting data for {calendar.month_name[month]}, {year}")
                raw_data=extract(color,year,month)             
                data=transform_data(raw_data,year,month)

                print(f"Starting ingestion to postgres for {calendar.month_name[month]}, {year}...")
                start_time=time()
                data.to_sql(name=table_name,con=engine, if_exists='append',index=False, chunksize=25000)
                print(f"Data ingestion for {calendar.month_name[month]}, {year} successful")
                print('Total time of ingestion: {time} seconds'.format(time=time()-start_time))
            else:
                print(f"Dataset already contains data for {calendar.month_name[month]}, {year}")
        except:
            print(f"Encountered an error")


    conn.close()

    return



@flow(log_prints=True)
def main_etl_flow(months:list, years:list,colors:list):
    for color in colors:
        for year in years:
            for month in months:
                ingest_data(color, year, month)

if __name__=='__main__':
    colors=["green","yellow"]
    months=list(range(1,12))
    years=[2022]
    main_etl_flow(months, years, colors)

