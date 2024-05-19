import logging
import os
import sys
from datetime import datetime
import glob
import logging

import apache_beam as beam  # type: ignore
import geopandas as gpd  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from shapely.geometry import Point  # type: ignore

from airflow.models import Variable
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.expanduser("~/CS5830/Assignment2/"))

from utils.pipeline_2 import (
    
    process_and_write_hourly_data,
    calculate_and_write_monthly_averages,
    set_task_inputs,
    make_heatmaps,
)

working_dir = os.path.expanduser("~/CS5830/Assignment2/")
os.makedirs(working_dir + "tmp", exist_ok=True)

# create dag
with DAG(
    dag_id="mrityunjay_analytics",
    schedule="@daily",
    start_date=datetime(year=2022, month=1, day=31),
    end_date=datetime(year=2025, month=2, day=28),
    catchup=False,
    tags=["test"],
) as dag:
    
    # Get the inputs from the user
    set_path_inputs = PythonOperator(
        task_id="set_path_inputs", python_callable=set_task_inputs, dag=dag
    )

    # Create variable to get the zip file path entered from user
    filepath = Variable.get("archive_file_path", default_var="/ARCCC/data_files.zip")
    logging.info(f"filepath later is {filepath}")

    # CHECK if the file exists
    if os.path.exists(filepath):
        logging.warning(f"VALIDATED The file at {filepath} exists.")
    else:
        error_msg = f"The file at {filepath} does not exist."
        logging.error(error_msg)
        exit()

    # Wait for the archive to be available
    archive = FileSensor(
        task_id="archive",
        filepath=working_dir + filepath,
        poke_interval=10,
        timeout=5,
        dag=dag,
    )

    # Check if the file is a valid archive followed by unzip
    validate_and_unzip = BashOperator(
        task_id="val_and_unzip",
        bash_command=f"""
        if [ -f {working_dir + filepath} ]; then
            unzip -o {working_dir + filepath} -d {working_dir}/tmp/
            echo "success"
        else 
            echo "error"
        fi 
        """,
        dag=dag,
    )

    # Extract CSV into data frame also the Lat/Long values in tuple
    process_df_hourly = PythonOperator(
        task_id="process_hourly_data",
        python_callable=process_and_write_hourly_data,
    )

    # compute the monthly averages of the required fields in tuple
    calculate_df_monthly_avg = PythonOperator(
        task_id="calculate_df_monthly_avg", python_callable=calculate_and_write_monthly_averages
    )

    # generate heatmaps for the required fields
    make_heatmaps = PythonOperator(
        task_id="heatmaps_build", python_callable=make_heatmaps
    )

    # delete tmp directory
    delete_tmp = BashOperator(
        task_id="del_tmp",
        bash_command=f"""rm -r {working_dir}/tmp""",
        dag=dag,
    )

    # define dependencies
    (
        set_path_inputs
        >> archive
        >> validate_and_unzip
        >> process_df_hourly
        >> calculate_df_monthly_avg
        >> make_heatmaps
        >> delete_tmp
    ) # 
