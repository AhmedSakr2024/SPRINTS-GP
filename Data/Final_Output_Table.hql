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







