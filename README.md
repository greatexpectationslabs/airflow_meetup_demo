# README

## Setup

This demo builds from the apache airflow quick start (https://airflow.apache.org/start.html).

Set `AIRFLOW_HOME` to point to the `airflow/` directory in this repository.


```
# airflow initdb
airflow webserver -p 8080
airflow scheduler

```
## Analysis

We will evaluate NPI data from cms.gov. For the original raw data, see: .

1. Install GE in our project, and profile the datasource.
2. Review data-docs built for the NPI data.
3. Identify some columns to investigate further:
   - "Provider Other Organization Name Type Code"
   - "Provider Enumeration Date"
4. Run the `create expectations` notebook




data_asset_name = 'demo__dir/default/npidata_pfile'
data_asset_name = 'demo__dir/default/npidata_pfile_transformed'


ge.dataset.util.build_categorical_partition_object(batch, column='Provider Other Organization Name Type Code')
batch.expect_column_distinct_values_to_be_in_set(column='Provider Other Organization Name Type Code', value_set=[3, 4, 5])