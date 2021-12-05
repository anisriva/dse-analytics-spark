#!/usr/bin/env bash
#################################################################                                                    
# Use this to shutdown the cassandra cluster inhouse            #                                                   
# 1 transactional and 1 analytics node                          #                                       
# Usage :                                                       #          
# sh shutdown.sh                                                #                               
#                                                               #  
# ex :                                                          #        
#     sh shutdown.sh : Normal start                             #                                                                           
#                                                               #  
# Author : Animesh Srivastava                                   #                              
#                                                               #  
#################################################################                                                    
export HOME=$(dirname "$0")
compose_path=$HOME/docker-compose.yaml
docker-compose -f $compose_path down