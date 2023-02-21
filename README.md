# nyc-taxi-ingestion

The Dockerized Data Pipeline performs the following tasks:

1. It retrieves taxi trips record dataset from the [NYC Taxi website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) in the form of a parquet file.

2. The dataset undergoes preliminary cleaning.

3. The processed data is ingested into the database only if the data is absent in the table.

4. A Prefect container is created to display the workflow user interface for the ingestion script.

5. A container running pgAdmin is set up to carry out additional data cleaning using SQL commands.



### Ingestion Script Workflow 
* [Python Script](https://github.com/fxboakye/nyc-taxi-ingestion/blob/master/flows/pg_ingestion.py)

* The script takes three parameters, which are taxi color, month, and year. Once the parameters are set, the script establishes a connection to the database and checks for any existing tables. If the required table does not exist, the script creates a new table based on the specified parameters. The table name is constructed as {color}_{year}_tripdata, such as green_2022_tripdata.

* Using the parameters, the script constructs a URL to the required New York Taxi dataset and creates a DataFrame from the data obtained from the URL. The script applies some data transformation and inserts the resulting data into the table. If the required table already exists, the script checks the table to verify if the dataset is not already in the table before calling the extraction and transformation process. This helps to avoid unnecessary computations and saves time.

* Moreover, if new parameters are provided, the script checks the database to ensure that the specified dataset does not already exist in the database. The script only performs data extraction and transformation from the URL if the data does not already exist in the database. This approach saves computing time for data sets obtained from the URL.

* In summary, the script automates the process of creating tables, inserting data, and verifying whether data already exists in the database. It uses efficient methods to minimize computation time and maximize efficiency.


##### Regarding this project, I've chosen to use the New York Taxi dataset for the year 2022, specifically from January to November.

### Next Steps

Will Utilize dbt core as an ELT process to carry out additional transformation on the dataset before visualization
