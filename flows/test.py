import pandas as pd

data=pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet")
records=data.head(2).to_records(index=False)
values=[str(i) for i in list(records)]
j=','.join(values)
print(values)
#
#
print(j)
#
#
print(records)
