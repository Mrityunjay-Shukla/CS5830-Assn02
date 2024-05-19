import os
import sys
from datetime import datetime
import logging
import random
import shutil

import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# sys.path.append('~/CS5830/Assignment2/')
sys.path.append(os.path.expanduser("~/CS5830/Assignment2/"))

from utils.pipeline_1 import (
    select_random_csv_urls,
    download_sampled_csv_files,
    create_csv_archive,
    move_archive_to_destination
)

working_dir = os.path.expanduser("~/CS5830/Assignment2/")


# create dag
with DAG(
    dag_id="mrityunjay",
    schedule="@daily",
    start_date=datetime(year=2022, month=1, day=31),
    end_date=datetime(year=2025, month=2, day=28),
    catchup=False,
    tags=["test"],
) as dag:

    # get page contents using wget
    get_page = BashOperator(
        task_id="get_page",
        bash_command="""wget -O ~/CS5830/Assignment2/tmp/datasets_new.html 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{{dag_run.conf["year"] if dag_run else ""}}'
        """,
    )

    # select files from  HTML randomly
    select_files = PythonOperator(
        task_id="select_files_from_fetched_html",
        python_callable=select_random_csv_urls,
        provide_context=True,
    )

    #to download the selected files
    fetch_csv_ = PythonOperator(
        task_id="fetch_individual_csv",
        python_callable=download_sampled_csv_files,
        provide_context=True,
    )

    # create zip archive 
    zip_files = PythonOperator(
        task_id="zip_files_arc",
        python_callable=create_csv_archive,
        provide_context=True,
    )

    # place zip to a location
    place_archive = PythonOperator(
        task_id="place_zip",
        python_callable=move_archive_to_destination,
        provide_context=True,
    )

    # to delete the tmp directory
    delete_tmp_dir = BashOperator(
        task_id="delete_tmp",
        bash_command=f"""rm -r {working_dir}/tmp""",
        dag=dag,
    )

    # define dependencies
    (
        get_page
        >> select_files
        >> fetch_csv
        >> zip_files
        >> place_archive
        >> delete_tmp_dir
    ) # type:ignore
