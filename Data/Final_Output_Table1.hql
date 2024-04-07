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