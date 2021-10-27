#!/bin/bash
#################################################################                                                    
# Use this to start the cassandra cluster inhouse               #                                                   
# 1 transactional and 1 analytics node                          #                                       
# Usage :                                                       #          
# sh start.sh <option> [fresh]                                  #                               
#  :param - fresh - Cleanup mounted data                        #                                         
#                                                               #  
# ex :                                                          #        
#     sh start.sh : Normal start                                #                                 
#     sh start.sh : Fresh start with cleanup                    #                                             
#                                                               #  
# Author : Animesh Srivastava                                   #                              
#                                                               #  
#################################################################                                                    
export HOME=`pwd`
compose_path=$HOME/docker-compose.yaml

check_server() {
    servers_up=`docker exec trans-seed dsetool status | grep UN | wc -l`
    if [ $servers_up != "$1" ]
    then
        echo "Waiting for the server to come up"
        sleep 5
        check_server $1
    else
        echo "Node --> $1 came up"
    fi
}

cleanup(){
    if [ "$1" == "fresh" ] && [ -d "mnt" ]
    then
        echo "Cleaning up the directory"
        rm -rf mnt/
    else
        echo 'Normal Start'
    fi
}

echo "Tearing down any existing setup"
docker-compose -f $compose_path down

cleanup $1

echo 'Starting transactional seed node'
docker-compose -f $compose_path up -d \
--scale trans-seed=1 \
--scale analytics-seed=0 \
--scale ds-studio=1 \
--scale postgres=1 \
--scale scheduler=1 \
--scale webserver=1 \
--remove-orphans

check_server 1

echo 'Starting analytical seed node'
docker-compose -f $compose_path up -d \
--scale analytics-seed=1 

check_server 2

while ! docker exec analytics-seed grep -q "DSE startup complete" //var//log//cassandra//system.log
do
  sleep 10
  echo 'Waiting for Analytics cluster to stabalize'
done;


echo 'All nodes started, setting up all the spark related keyspaces' 

sleep 60

docker exec trans-seed cqlsh -u cassandra -p cassandra -e "ALTER KEYSPACE cfs WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE cfs_archive WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE dse_leases WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE dsefs WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE \"HiveMetaStore\" WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE spark_system WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};" 

while !  docker exec analytics-seed grep -q "SPARK-WORKER service is running" //var//log//spark//worker//worker.log
do
  sleep 10
  echo 'Waiting for Spark Worker to start'
done;

while ! docker exec analytics-seed grep -q "SPARK-MASTER service is running" //var//log//spark//master//master.log
do
  sleep 10
  echo 'Waiting for Spark Master to start'
done;

echo "Do all the node repair"
docker exec trans-seed nodetool repair -full
docker exec analytics-seed nodetool repair -full

if [ -d "mnt\cassandra\analytics-seed\cassandra\data\PortfolioDemo" ]
then
    echo "Keyspace already created skipping creation"
else

    echo "Creating portfolio schema in the transactional datacenter"
    docker exec trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o INSERT_PRICES
    docker exec trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o UPDATE_PORTFOLIOS
    docker exec trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o INSERT_HISTORICAL_PRICES -n 100
    
    echo "Altering portfolio keyspace"
    
    docker exec trans-seed \
     cqlsh -u cassandra \
     -p cassandra \
     -e "ALTER KEYSPACE \"PortfolioDemo\" WITH replication = {'class': 'NetworkTopologyStrategy', 'trans':1, 'analytics':1};"
    
    echo "Do all the node for portfolio"
    docker exec trans-seed nodetool repair PortfolioDemo
    docker exec analytics-seed nodetool repair PortfolioDemo
fi

echo "Starting thrift server"
docker exec analytics-seed \
dse spark-sql-thriftserver start --hiveconf \
hive.server2.thrift.client.user=dse \
hive.server2.thrift.client.password=dse \
--conf spark.cores.max=2

echo "Starting jupyter notebook"
docker exec analytics-seed sh //opt//dse//resources//spark//bin//start_jupyter.sh

echo "Use below link to login to jupyter notebook:
        http://localhost:8888
"

echo "Connection String to connect to spark sql using sql clients : 
    url : jdbc:hive2://localhost:10000/PortfolioDemo
    uname : dse
    password : dse
"