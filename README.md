# SPRINTS-GP
SPRINTS Graduation_Project about Covid-19 CSV Analysis
---------------- GP_Task -------------------------------------
1- Cloudera VM
````````````````
mkdir -p /home/cloudera/covid_project
mkdir -p /home/cloudera/covid_project/scripts
mkdir -p /home/cloudera/covid_project/landing_zone/COVID_SRC_LZ
=> Upload dataset “covid-19.csv”
mkdir -p /home/cloudera/uploaded_data/


-  I had make sure connection between my_PC and Cloudera VM  is successfully sewtup to transfer files by using WINSCP.exe

2 HDFS Directories
`````````````````

hdfs dfs -mkdir -p /user/cloudera/ds/COVID_HDFS_LZ
hdfs dfs -mkdir -p /user/cloudera/ds/COVID_HDFS_PARTITIONED
hdfs dfs -mkdir -p /user/cloudera/ds/COVID_FINAL_OUTPUT


hdfs dfs -put -p /home/cloudera/covid_project/landing_zone/COVID_SRC_LZ/covid-19.csv /user/cloudera/ds/COVID_HDFS_LZ/

hdfs dfs -ls /user/hue/oozie/workspaces/hue-oozie-1711163304.09/lib

hdfs dfs -put -p /home/cloudera/uploaded_data/Load_COVID_TO_HDFS.sh  /user/hue/oozie/workspaces/hue-oozie-1711163304.09/lib

hdfs dfs -put -p /home/cloudera/uploaded_data/run.sh  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Load_COVID_TO_HDFS.sh  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Staging_Table.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Partioned_Table.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Final_Output_Table.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Final_Output_Table1.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Final_Output_Table2.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Final_GP2.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib
hdfs dfs -put -p /home/cloudera/uploaded_data/Load_into_Staging.hql  /user/hue/oozie/workspaces/hue-oozie-1711870505.29/lib


3- Create DB on Hive
`````````````````````
DROP DATABASE IF EXISTS covid_db CASCADE;
CREATE database covid_db;

use covid_db;

create table if not exists covid_db.covid_staging
(
 Country                            STRING,
 Total_Cases                        DOUBLE,
 New_Cases                          DOUBLE,
 Total_Deaths                       DOUBLE,
 New_Deaths                         DOUBLE,
 Total_Recovered                    DOUBLE,
 Active_Cases                       DOUBLE,
 Serious                            DOUBLE,
 Tot_Cases                          DOUBLE,
 Deaths                             DOUBLE,
 Total_Tests                        DOUBLE,
 Tests                              DOUBLE,
 CASES_per_Test                     DOUBLE,
 Death_in_closed_Cases              STRING,
 `Rank_by_Testing_rate`             DOUBLE,
 `Rank_by_Death_rate`               DOUBLE,
 `Rank_by_Cases_rate`               DOUBLE,
 `Rank_by_Death_of_Closed_Cases`    DOUBLE
 )
 ROW FORMAT DELIMITED 
 FIELDS  TERMINATED by ','
 STORED as TEXTFILE
 LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
 tblproperties ("skip.header.line.count"="1");

 ==> Use script "Load_into_Staging.hql" to insert data from csv file into table: Load_into_Staging.hql   

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned 
(
 Country 			                STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                       DOUBLE,
 New_Deaths                         DOUBLE,
 Total_Recovered                    DOUBLE,
 Active_Cases                       DOUBLE,
 Serious		                  	DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                 	DOUBLE,
 CASES_per_Test                     DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 `Rank_by_Testing_rate`             DOUBLE,
 `Rank_by_Death_rate`               DOUBLE,
 `Rank_by_Cases_rate`               DOUBLE,
 `Rank_by_Death_of_Closed_Cases`    DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
STORED as ORC
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';

SET hive.exec.dynamic.partition.mode=nonstrict;


FROM covid_db.covid_staging 
INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country AS COUNTRY_NAME
WHERE Country IS NOT NULL
limit 50;


-- select *
-- from covid_db.covid_ds_partitioned;


-- drop table covid_db.covid_final_output;

-- Step 1: Define the "covid_final_output" table schema
CREATE TABLE IF NOT EXISTS covid_db.covid_final_output 
(
    country 				STRING,
    Tot_Cases 			    DOUBLE,
    Total_Deaths 			DOUBLE,
    Total_Recovered		    DOUBLE,
	Total_Tests				DOUBLE,
	--TOP_DEATH 			    STRING,
   -- TOP_TEST 			    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';


-- Step 2: Write a query to generate the "covid_final_output"
INSERT OVERWRITE TABLE covid_db.covid_final_output
SELECT 
    country,
    SUM(tot_cases) AS Total_Cases,
    SUM(total_deaths) AS Total_Deaths,
    SUM(total_recovered) AS Total_Recovered,
    SUM(total_tests) AS Total_Tests
FROM 
    covid_db.covid_staging
GROUP BY 
    country;


-- Step 3: Export the query result to a file
INSERT OVERWRITE DIRECTORY  '/user/cloudera/ds/COVID_FINAL_OUTPUT'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM covid_db.covid_final_output;


