DROP DATABASE IF EXISTS covid_db CASCADE;
CREATE DATABASE covid_db;


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

CREATE TABLE IF NOT EXISTS covid_db.covid_final_output 
(
    country 				STRING,
    Tot_Cases 			    DOUBLE,
    Total_Deaths 			DOUBLE,
    Total_Recovered		    DOUBLE,
	Total_Tests				DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';


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



INSERT OVERWRITE DIRECTORY  '/user/cloudera/ds/COVID_FINAL_OUTPUT'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM covid_db.covid_final_output;



