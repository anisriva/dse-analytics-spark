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
from airflow.operators.dummy import DummyOperator

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
        "hello_world_dag",
        schedule_interval="*/15 * * * *",
        catchup=False,
        default_args=default_args
        ) 

with dag:
    # Loop topic list for consumer.py and creating tasks on the fly
    task1 = DummyOperator(task_id= "1")
    task2 = DummyOperator(task_id= "2")
    task3 = DummyOperator(task_id= "3")
    task4 = DummyOperator(task_id= "4")
    task5 = DummyOperator(task_id= "5")
    task6 = DummyOperator(task_id= "6")

task1 >> task4
task2 >> task4
task3 >> task4
task4 >> task5
task4 >> task6