'''
LAST_UPDATED_ON: 21/10/2021
'''
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *

__version__ = '1.0'
__author__ = ['Animesh']

import logging
from os import getenv
from os.path import join
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

log = logging.getLogger(__name__)

# spark config variables
spark_master = "spark://analytics-seed:7077"
deploy_mode = 'cluster'
driver_memory = 1
num_executors = 1
executor_memory = 1
executor_cores = 1

# Define the task level variables
task_name = 'apple_stocks'
spark_script = 'apple_stocks.py'

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

# Define dag level global variables
ssh_conn_id = 'spark-ssh'
local_spark_dir = join(getenv('AIRFLOW_HOME'),'dags/spark_scripts')
remote_spark_dir = '/var/lib/spark/scripts'
timeout = 30*60

cmd = f'''export JAVA_HOME=/usr/local/openjdk-8; \\
        /opt/dse/bin/dse spark-submit \\
        {join(remote_spark_dir, spark_script)} \\
        --master {spark_master} \\
        --deploy-mode {deploy_mode} \\
        --driver-memory {driver_memory}g \\
        --executor-memory {executor_memory}g \\
        --executor-cores {executor_cores} \\
        --num_executors {num_executors}
        '''

# Create dag object
dag = DAG(
        "spark_load_apple_stocks",
        schedule_interval=None,
        catchup=False,
        default_args=default_args
        ) 

with dag:
        move_code = SFTPOperator(
                        task_id = f'{task_name}_transfer_script',
                        ssh_conn_id = ssh_conn_id,
                        local_filepath = join(local_spark_dir, spark_script),
                        remote_filepath = join(remote_spark_dir, spark_script),
                        operation = "put",
                        create_intermediate_dirs=True
                        )
        run_code = SSHOperator(
                        task_id = f'{task_name}_run_script',
                        ssh_conn_id = ssh_conn_id,
                        command = cmd,
                        timeout = timeout,
                        do_xcom_push = True,
                        environment = {'JAVA_HOME':'/usr/local/openjdk-8'}
                        )

move_code >> run_code