CREATE KEYSPACE nitro 
WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'BLU-DEV': '3', 'BLU-DEV-ANALYTICS': '2' } 
AND DURABLE_WRITES = true;

USE nitro;

CREATE TABLE emp(
  empid int,
  depid int,
  empname text,
  salary int,
  PRIMARY KEY(empid, depid)
) WITH cdc=true;


CREATE TABLE dept(
  depid int,
  empid int,
  depname text,
  locationid int,
  PRIMARY KEY(depid, empid)
) WITH cdc=true;