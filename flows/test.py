#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, text
from time import time
from prefect import flow, task
from secret import postgres_credentials

@task()
def extract(url:str):

    data=pd.read_parquet(url)

    #changes all column headers to lowercase
    data.columns=[i.lower() for i in data.columns]
    print(data.dtypes)

    return data

@task()
def transform_data(data):
    print('Initial count of trips with zero passengers = {count}'.format(count=data['passenger_count'].isin([0]).sum()))

    #removing rows with no passengers
    print('removing the above records')
    new_data=data[data['passenger_count']!=0]

    print('New count of trips with zero passengers = {count}'.format(count=new_data['passenger_count'].isin([0]).sum()))

    return new_data


@task()
def ingest_data(table_name:str, data) -> None:

    #Postgres Credentials
    user=postgres_credentials.user
    pwd=postgres_credentials.password
    host=postgres_credentials.host
    db=postgres_credentials.database

    #Creates connection
    engine=create_engine(f'postgresql://{user}:{pwd}@{host}:{5432}/{db}')
    conn=engine.connect()
    
    print('Starting ingestion to postgres')
    start_time=time()
     
    data.to_sql(name=table_name,con=engine, if_exists='append',index=False, chunksize=25000)
    print('Total time of ingestion: {time} seconds'.format(time=time()-start_time))

    #Inserting data into table

    result=conn.execute(text(f"select count(*) from {table_name} "))
    print(result)
    for row in result:
        print(row)

    conn.close()

    return

@flow(log_prints=True)
def main_flow():
    table_name="green_trip" 
    parquet_url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
    raw_data=extract(parquet_url)
    transformed_data= transform_data(raw_data)
    ingest_data(table_name, transformed_data)

if __name__=='__main__':
    main_flow()