#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, text
from time import time
import calendar
from prefect import flow, task
from secret import postgres_credentials

@task()
def extract(color:str, year:int, month:int) -> pd.DataFrame:
    """Creates a url from colour, year and month parameters.
    Extracts Dataframe from URL.
    Performs premilinary data cleaning.
    """

    #settting valid arguments
    valid_colors=["green", "yellow"]
    valid_years=list(range(2019,2023))
    valid_months=list(range(1,13)) 

    if color not in valid_colors:
        raise ValueError("Colour must be yellow or green")
    elif year not in valid_years:
        raise ValueError("Year must be between 2019 and 2022") 
    elif month not in valid_months:
        raise ValueError("Month is out of range")
    else:
        parquet_url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet"
        data=pd.read_parquet(parquet_url)

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
def ingest_data(color:str, year:str, month:str, data:pd.DataFrame) -> None:
    """Connects to Postgres Database.
    Only adds data to the table when the specified months are not present in the database.
    """
    #Postgres Credentials
    user=postgres_credentials.user
    pwd=postgres_credentials.password
    host=postgres_credentials.host
    db=postgres_credentials.database

    #Creates connection
    engine=create_engine(f'postgresql://{user}:{pwd}@{host}:{5432}/{db}')
    conn=engine.connect()
    
    table_name=f"{color}_{year}_tripdata"
    print('Starting ingestion to postgres')

    try:
        start_time=time()
        data.to_sql(name=table_name,con=engine, if_exists='fail',index=False, chunksize=25000)
        print('Total time of ingestion: {time} seconds'.format(time=time()-start_time))
    except:
        try:
            print(f"{table_name} already exists")
            print("Checking if month exists in table....")
            #Returns list containing distinct months in dataset in the form (month,)
            result=conn.execute(text(f"SELECT DISTINCT EXTRACT(MONTH FROM PICKUP_DATETIME) FROM {table_name}"))
            months_in_db=[row for row in result]
            print(months_in_db)
            print((float(month),))
            if (float(month),) not in months_in_db:
                print(f"Inserting data for {calendar.month_name[month]}")
                start_time=time()
                data.to_sql(name=table_name,con=engine, if_exists='append',index=False, chunksize=25000)
                print('Total time of ingestion: {time} seconds'.format(time=time()-start_time))
            else:
                print(f"Dataset already contains data for {calendar.month_name[month]}")
        except:
            print("Issues")

    conn.close()

    return

@flow(log_prints=True)
def etl_to_pg(month:int, year:int, color: str):
    raw_data=extract(color,year,month)
    data=transform_data(raw_data, year,month)
    ingest_data(color,year,month,data)

    return len(data)



@flow(log_prints=True)
def main_etl_flow(months:list, years:list,colors:list):
    total=0
    for color in colors:
        for year in years:
            for month in months:
                row_count=etl_to_pg(month, year, color)
                total +=row_count
    print(f'Total rows ingested={total}')

if __name__=='__main__':
    colors=["green"]
    months=list(range(1,5))
    years=[2019]
    main_etl_flow(months, years,colors)