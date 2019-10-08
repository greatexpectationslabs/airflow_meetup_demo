import os

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG


class DbtOperator(BashOperator):
    """
    DBT Operator for airflow.
    It's a wrapper used to generate a BashOperator with a 'dbt run' command.
    """
    def __init__(
            self,
            profile,
            target,
            models,
            exclude_models=None,
            project_dir=None,
            profiles_dir=None,
            *args,
            **kwargs):

        models = [models] if type(models) is not list else models
        exclude_models = [exclude_models] if exclude_models and type(exclude_models) is not list else exclude_models

        exclude_models_arg = f' --exclude {" ".join(exclude_models)}' if exclude_models else ''
        project_dir_arg = f' --project-dir {project_dir}' if project_dir else ''
        profiles_dir_arg = f' --profiles-dir {profiles_dir}' if profiles_dir else ''

        # Builds DBT run command.
        # DBT
        dbt_command = f'dbt run --models {" ".join(models)} --profile {profile} --target {target} {exclude_models_arg} {project_dir_arg} {profiles_dir_arg}'

        super().__init__(bash_command=dbt_command, *args, **kwargs)

        self.profile = profile
        self.target = target
        self.models = models
        self.exclude_models = exclude_models
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir


def create_dbt_task(dag, dbt_model, profile, target):
    """
    Creates a DBT task to run the model(s) provided
    """

    return DbtOperator(
        profile=profile,
        target=target,
        models=[dbt_model],
        dag=dag,
        task_id=dbt_model,
    )


args = {
    "owner": "james@greatexpectations.io",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id="GE_Airflow_Demo",
    default_args=args,
    schedule_interval=None,
)

import great_expectations as ge
from great_expectations import DataContext


def get_ge_context():
    """
    Instantiates the Great Expectations (GE) and returns the DataContext object to be used as a handle for performing data validations.
    Great Expectations (https://github.com/great-expectations/great_expectations) is an Open Source library maintained by Superconductive Health, Inc. (https://www.superconductivehealth.com/).
    Great Expectations is a purpose fit framework and Python library for data understanding and data validation used by Data Enginerring, Data Science, and Machine Learning engineers worldwide.
    """
    context_root_dir: str = os.getenv("GE_HOME")
    ge_data_context: DataContext = ge.data_context.DataContext(context_root_dir=context_root_dir)
    return ge_data_context


def validate():
    """
    Perform validations of the previously defined data assets according to their respective expectation suites.
    """
    import json
    from typing import Dict
    from airflow import AirflowException

    schema_name: str = """compute schema name from environment and/or profile (e.g., DBT profile/target) and/or configuration"""

    data_asset_name_config_dict: Dict[str, Dict[str, list[str]]] = {
        "asset_1": {"table_name": "table_1", "expectation_suite_names_list": ["expectations_suite_11", "expectations_suite_12", "expectations_suite_13", ]},
        "asset_2": {"table_name": "table_2", "expectation_suite_names_list": ["expectations_suite_21", "expectations_suite_22", "expectations_suite_23", ]},
        "asset_3": {"table_name": "table_3", "expectation_suite_names_list": ["expectations_suite_31", "expectations_suite_32", "expectations_suite_33", ]},
    }

    ge_data_context: DataContext = get_ge_context()

    run_id="something generic and unique like UTC timestamp"

    for (data_asset_name, config_dict) in data_asset_name_config_dict.items():
        table_name: str = config_dict["table_name"]
        expectation_suite_names_list: list = config_dict["expectation_suite_names_list"]
        for expectation_suite_name in expectation_suite_names_list:
            batch: ge.dataset.Dataset = ge_data_context.get_batch(
                data_asset_name=data_asset_name,
                expectation_suite_name=expectation_suite_name,
                batch_kwargs={"schema": schema_name, "table": table_name}
            ) 
            validation_result: Dict[str, str] = batch.validate(run_id=run_id)
            if not validation_result["success"]:
                raise AirflowException(str(validation_result))



# Create tasks to run DBT models
# These DBT models must exist in a DBT project implementing the "T" of the ELT system.
# AirFlow tasks can be create for every model in DBT DAG or models that refer to other models, causing them to be computed.
# The granularity of AirFlow tasks to DBT models is optimized for unit testing considerations during development phase; it can be relaxed for production.

model_1 = create_dbt_task(dag, 'sessions_raw', 'foo', 'bar')
model_2 = create_dbt_task(dag, 'sessions_data', 'foo', 'bar')
model_3 = create_dbt_task(dag, 'account_updates_raw', 'foo', 'bar')
model_4 = create_dbt_task(dag, 'account_updates_clean', 'foo', 'bar')
model_5 = create_dbt_task(dag, 'sessions_check_asset', 'foo', 'bar')
model_6 = create_dbt_task(dag, 'previous_sessions', 'foo', 'bar')
model_7 = create_dbt_task(dag, 'previous_sessions_check_asset', 'foo', 'bar')
model_8 = create_dbt_task(dag, 'sessions_account_log', 'foo', 'bar')

publish = create_dbt_task(dag, 'publish', 'foo', 'bar')

# Validation task
validate_operator = PythonOperator(
    task_id='final_audit',
    python_callable=validate,
    op_kwargs={},
    dag=dag,
)


drop1_validate_operator = PythonOperator(
    task_id='session_validation',
    python_callable=validate,
    op_kwargs={},
    dag=dag
)

drop2_validate_operator = PythonOperator(
    task_id='account_updates_validation',
    python_callable=validate,
    op_kwargs={},
    dag=dag
)

# Dependencies
drop1_validate_operator >> model_1 >> [model_2, model_5] >> validate_operator
drop2_validate_operator >> model_3 >> model_4 >> validate_operator
[model_6, model_7] >> validate_operator
model_8 >> validate_operator

validate_operator >> publish

# After the validate operator completes execution, the "P" of "WAP" is determined.  If there was no exception in validation, then the temporary schema can be moved into permanent schema; otherwise, Slack alert is sent for analysis.
