INSERT OVERWRITE DIRECTORY  '/user/cloudera/ds/COVID_FINAL_OUTPUT'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM covid_db.covid_final_output;