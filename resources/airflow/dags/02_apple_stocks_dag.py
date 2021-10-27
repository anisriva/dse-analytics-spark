'''
LAST_UPDATED_ON: 21/10/2021
'''
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *

__version__ = '1.0'
__author__ = ['Animesh']

import logging
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

log = logging.getLogger(__name__)

# Defining the dag arguments
default_args={
            "owner": "airflow",
            "start_date": datetime(2021, 3, 5),
            "retry_delay": timedelta(seconds=5),
            "sla": timedelta(hours=23),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 10
            }

# Create dag object
dag = DAG(
        "spark_load_apple_stocks",
        schedule_interval="*/15 * * * *",
        catchup=False,
        default_args=default_args
        ) 

with dag:
    spark_task = SparkSubmitOperator(
                task_id = 'spark_apple_stocks',
                conn_id = 'analytics_seed',
                application = '/opt/airflow/dags/spark_scripts/apple_stocks.py',
                verbose = True

    )
