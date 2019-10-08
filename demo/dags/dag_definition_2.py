from datetime import datetime

import logging

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

from great_expectations import DataContext
from great_expectations.datasource.types import BatchKwargs

args = {
    "owner": "james@greatexpectations.io",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id="GE_Airflow_Demo__MEETUP",
    default_args=args,
    schedule_interval=None,
)


def validate(data_asset_name, expectation_suite_name, batch_kwargs):
    """
    Perform validations of the previously defined data assets according to their respective expectation suites.
    """

    context: DataContext = DataContext('/Users/james/dev/presentations/201910_nyc_airflow_meetup/project_src/great_expectations')
    context.run_validation_operator(
        validation_operator_name='action_list_operator',
        assets_to_validate=[(data_asset_name, expectation_suite_name, batch_kwargs)],
        run_identifier=datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    )


def demo_transform(infile, outfile):
    """
    Peform a simple demo transform
    :param infile:
    :param outfile:
    :return:
    """
    import pandas as pd

    df = pd.read_csv(infile)
    df['new_column'] = ['live_demos_are_scary!'] * len(df)
    df.to_csv(outfile)


# Validation task
raw_validation = PythonOperator(
    task_id='raw_validation',
    python_callable=validate,
    op_kwargs={
        'data_asset_name': 'demo__dir/default/npidata_pfile',
        'expectation_suite_name': 'warning',
        'batch_kwargs': BatchKwargs(
            path='/data/demo/npidata_pfile/npidata_pfile_20050523-20190908_0.csv'
        )
    },
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=demo_transform,
    op_kwargs={
        'infile': '/data/demo/npidata_pfile/npidata_pfile_20050523-20190908_0.csv',
        'outfile': '/data/demo/npidata_pfile/npidata_pfile_20050523-20190908_100.csv'
    },
    dag=dag
)

final_validation = PythonOperator(
    task_id='final_validation',
    python_callable=validate,
    op_kwargs={
        'data_asset_name': 'demo__dir/default/npidata_pfile_transformed',
        'expectation_suite_name': 'warning',
        'batch_kwargs': BatchKwargs(
            path='/data/demo/npidata_pfile/npidata_pfile_20050523-20190908_100.csv'
        )
    },
    dag=dag,
)

# Dependencies
raw_validation >> transform >> final_validation