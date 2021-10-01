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

compose_path='docker-compose.yaml'

check_server() {
    servers_up=`docker exec -t trans-seed dsetool status | grep UN | wc -l`
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
--remove-orphans

check_server 1

echo 'Starting analytical seed node'
docker-compose -f $compose_path up -d \
--scale analytics-seed=1 

check_server 2

echo 'All nodes started, setting up all the spark related keyspaces' 

sleep 10

docker exec -t trans-seed cqlsh -u cassandra -p cassandra -e "ALTER KEYSPACE cfs WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE cfs_archive WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE dse_leases WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE dsefs WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE \"HiveMetaStore\" WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};
ALTER KEYSPACE spark_system WITH replication = {'class': 'NetworkTopologyStrategy', 'analytics':1};" 

echo "Do all the node repair"
docker exec -t trans-seed nodetool repair -full
docker exec -t analytics-seed nodetool repair -full

if [ -d "mnt\cassandra\analytics-seed\cassandra\data\PortfolioDemo" ]
then
    echo "Keyspace already created skipping creation"
else

    echo "Creating portfolio schema in the transactional datacenter"
    docker container exec -it trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o INSERT_PRICES
    docker container exec -it trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o UPDATE_PORTFOLIOS
    docker container exec -it trans-seed //opt//dse//demos//portfolio_manager//bin//pricer -o INSERT_HISTORICAL_PRICES -n 100
    
    echo "Altering portfolio keyspace"
    
    docker exec -t trans-seed \
     cqlsh -u cassandra \
     -p cassandra \
     -e "ALTER KEYSPACE \"PortfolioDemo\" WITH replication = {'class': 'NetworkTopologyStrategy', 'trans':1, 'analytics':1};"
    
    echo "Do all the node for portfolio"
    docker exec -t trans-seed nodetool repair PortfolioDemo
    docker exec -t analytics-seed nodetool repair PortfolioDemo
fi

echo "Run below command to start jupyter:
--------------------------------------
1. Enter the docker container 

    docker container exec -it analytics-seed bash

2. Go to the working directory

    cd /var/lib/spark/jupyter

3. Run the jupyter notebook

    cd /var/lib/spark/jupyter
    nohup jupyter notebook --ip=analytics-seed --port=8888 --NotebookApp.token='' --NotebookApp.password='' &

4. exit
"