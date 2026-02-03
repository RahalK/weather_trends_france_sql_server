-- Section 1: Retrieve data
-- Retrieve data from the 'temperature_france' table
SELECT * 
FROM Portfolio_project..temperature_france;

-- Retrieve data from the 'weather_france' table
SELECT * 
FROM Portfolio_project..weather_france;


-- Section 2: Count total records
-- Count the total number of records in the 'temperature_france' table
SELECT COUNT(*) AS total_records
FROM Portfolio_project..temperature_france;

-- Count the total number of records in the 'weather_france' table
SELECT COUNT(*) AS total_records
FROM Portfolio_project..weather_france;


-- Section 3: Rename date columns
-- Change 'datetime' to 'date_and_time'
EXEC sp_rename 'temperature_france.datetime', 'date_and_time', 'COLUMN';
EXEC sp_rename 'weather_france.datetime', 'date_and_time', 'COLUMN';


-- Section 4: Correct the data types using the ALTER TABLE statement whenever Microsoft SQL Server Management Studio
-- incorrectly identifies them
-- 'temperature_france' table
ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN lat FLOAT;

ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN lon FLOAT;

ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN date_and_time DATETIME;

ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN temperature FLOAT;

ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN max_temperature FLOAT;

ALTER TABLE Portfolio_project..temperature_france
ALTER COLUMN min_temperature FLOAT;

-- 'weather_france' table
ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN lat FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN lon FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN date_and_time DATETIME;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN wind_speed FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN wind_direction FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN mean_sea_level_pressure FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN precipitation_24_hours FLOAT;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN uv_index NVARCHAR(50);

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN sunrise DATETIME;

ALTER TABLE Portfolio_project..weather_france
ALTER COLUMN sunset DATETIME;


-- Section 5: Seasonal trends
-- Count the number of records for each day, considering each timestamp separately
SELECT
    DAY(CONVERT(datetime, date_and_time)) as day,
    COUNT(*) AS records_count
FROM Portfolio_project..weather_france wf 
GROUP BY DAY(CONVERT(datetime, date_and_time))
ORDER BY 1;


-- Section 6: Data manipulation
-- Add the 'coordinates' column to 'temperature_france' table
ALTER TABLE Portfolio_project..temperature_france
ADD coordinates VARCHAR(50);

-- Populate the 'coordinates' column with latitude and longitude information
UPDATE Portfolio_project..temperature_france 
SET coordinates = '(' + COALESCE(CONVERT(VARCHAR, lat), '') + ', ' + COALESCE(CONVERT(VARCHAR, lon), '') + ')';

-- Add the 'coordinates' column to 'weather_france' table
ALTER TABLE Portfolio_project..weather_france
ADD coordinates VARCHAR(50);

-- Populate the 'coordinates' column with latitude and longitude information
UPDATE Portfolio_project..weather_france 
SET coordinates = '(' + COALESCE(CONVERT(VARCHAR, lat), '') + ', ' + COALESCE(CONVERT(VARCHAR, lon), '') + ')';

-- Retrieve distinct sets of coordinates from the 'weather_france' table
SELECT DISTINCT coordinates
FROM Portfolio_project..weather_france; 

-- Add a new column 'city' to the 'weather_france' table
ALTER TABLE Portfolio_project..weather_france
ADD city NVARCHAR(255);

-- Update the 'city' column based on the coordinates
UPDATE Portfolio_project..weather_france
SET city = 
    CASE
        WHEN coordinates = '(48.8566, 2.3522)' THEN 'Paris'
        WHEN coordinates = '(45.764, 4.8357)' THEN 'Lyon'
		WHEN coordinates = '(43.7102, 7.262)' THEN 'Nice'
        ELSE 'Unknown'
    END;

-- Retrieve distinct sets of coordinates and cities from the 'weather_france' table
SELECT DISTINCT coordinates, city
FROM Portfolio_project..weather_france; 


-- Section 7: Date ranges
-- Extract date ranges for the 'temperature_france' table
SELECT 
    MAX(date_and_time) as max_date, 
    MIN(date_and_time) as min_date, 
    CASE 
        WHEN ISDATE(MIN(date_and_time)) = 1 AND ISDATE(MAX(date_and_time)) = 1
            THEN CAST(DATEDIFF(day, MIN(date_and_time), MAX(date_and_time)) AS NVARCHAR) + ' days'
        ELSE 'Invalid date'
    END AS date_difference
FROM Portfolio_project..temperature_france;

-- Extract date ranges for the 'weather_france' table
SELECT 
    MAX(date_and_time) as max_date, 
    MIN(date_and_time) as min_date, 
    CASE 
        WHEN ISDATE(MIN(date_and_time)) = 1 AND ISDATE(MAX(date_and_time)) = 1
            THEN CAST(DATEDIFF(day, MIN(date_and_time), MAX(date_and_time)) AS NVARCHAR) + ' days'
        ELSE 'Invalid date'
    END AS date_difference
FROM Portfolio_project..weather_france;


-- Section 8: Top 20 Temperature Rankings by Location
-- Analyse temperature rankings using DENSE_RANK and a CTE for latitude and longitude, displaying the top 20 records
WITH rank_temperature (latitude, longitude, date_and_time, temperature, temperature_dense_rank) AS (
SELECT 
    lat,
    lon,
    date_and_time,
    temperature,
    DENSE_RANK() OVER (PARTITION BY lat, lon ORDER BY temperature DESC)
FROM Portfolio_project..temperature_france tf
)

SELECT latitude, longitude, date_and_time, temperature_dense_rank
FROM rank_temperature
WHERE temperature_dense_rank <= 20;


-- Section 9: Percentile calculation
-- Calculate median temperature for each location
SELECT 
    coordinates,
    temperature,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature) OVER (PARTITION BY lat, lon) AS median_temperature
FROM Portfolio_project..temperature_france;


-- Section 10: Previous and next temperature trends
-- Analyse the 3 previous temperature trends over time for each location, where temperature is higher than the 
-- average temperature for the given day
SELECT 
    coordinates,
    FORMAT(date_and_time, 'yyyy-MM-dd HH:mm') date_and_time,
    temperature,
    LAG(temperature, 3) OVER (PARTITION BY coordinates ORDER BY date_and_time) AS three_prev_temperature
FROM Portfolio_project..temperature_france
WHERE temperature > (
    SELECT AVG(temperature)
    FROM Portfolio_project..temperature_france AS sub
);

-- Analyse next temperature trends over time based on the uv_index
SELECT
    uv_index,
    FORMAT(date_and_time, 'yyyy-MM-dd HH:mm') date_and_time,
    LEAD(wind_speed) OVER (PARTITION BY uv_index ORDER BY date_and_time DESC) AS next_wind_speed
FROM Portfolio_project..weather_france;


-- Section 11: Aggregated weather statistics
-- Calculate aggregated weather statistics for each day and each city
SELECT 
    DAY(tf.date_and_time) AS day,
    wf.city,
    ROUND(AVG(wf.wind_speed), 2) AS avg_wind_speed,
    ROUND(AVG(wf.wind_direction), 2) AS avg_wind_direction,
    ROUND(AVG(wf.mean_sea_level_pressure), 2) AS avg_mean_sea_level_pressure, 
    ROUND(AVG(wf.precipitation_24_hours), 2) AS avg_precipitation_24_hours
FROM Portfolio_project..temperature_france tf
INNER JOIN 
    Portfolio_project..weather_france wf ON tf.lat = wf.lat 
                      AND tf.lon = wf.lon	
                      AND tf.date_and_time = wf.date_and_time
GROUP BY DAY(tf.date_and_time), wf.city
ORDER BY day;


-- Section 12: Max temperature by day
-- Calculate maximum temperature for specific days at each location
SELECT 
    coordinates,
    MAX(CASE WHEN DAY(date_and_time) LIKE '1%' THEN temperature END) AS first_days_max_temperature,
    MAX(CASE WHEN DAY(date_and_time) LIKE '2%' THEN temperature END) AS last_days_max_temperature
FROM Portfolio_project..temperature_france tf 
GROUP BY coordinates;


-- Section 13: Daily average temperature
-- Calculate daily average temperature for each day
SELECT 
    CONVERT(DATE, date_and_time) AS extracted_date,
    ROUND(AVG(temperature), 2) AS avg_temperature
FROM Portfolio_project..temperature_france tf 
GROUP BY CONVERT(DATE, date_and_time)
ORDER BY extracted_date;


-- Section 14: Temperature anomalies
-- Categorize temperature values into 'High', 'Low', or 'Normal' based on deviations from average temperature
SELECT
    date_and_time AS date,
    temperature,
    CASE
        WHEN temperature > (SELECT AVG(temperature) FROM Portfolio_project..temperature_france) + 1 * (SELECT STDEV(temperature) FROM Portfolio_project..temperature_france) THEN 'High'
        WHEN temperature < (SELECT AVG(temperature) FROM Portfolio_project..temperature_france) - 1 * (SELECT STDEV(temperature) FROM Portfolio_project..temperature_france) THEN 'Low'
        ELSE 'Normal'
    END AS temperature_category
FROM Portfolio_project..temperature_france tf;


-- Section 15: Top 1 percent of maximum temperature
-- Identify the top 1 percent of geographic coordinates with the highest maximum temperature within a specific date range
SELECT TOP 1 PERCENT 
    tf.coordinates,  
    wf.city "corresponding city",
    MAX(tf.temperature) AS max_temperature
FROM Portfolio_project..temperature_france tf
JOIN Portfolio_project..weather_france wf
	ON tf.coordinates = wf.coordinates 
	AND tf.date_and_time = wf.date_and_time
WHERE CONVERT(VARCHAR(10), tf.date_and_time, 120) BETWEEN '2024-01-16' AND '2024-01-18'
GROUP BY tf.coordinates, wf.city
ORDER BY max_temperature DESC;


-- Section 16: Average temperature ranking
-- Calculate the average temperature for each location and rank them from highest to lowest
SELECT
    coordinates,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    RANK() OVER (ORDER BY AVG(temperature) DESC) AS temperature_rank
FROM Portfolio_project..temperature_france tf 
GROUP BY coordinates
ORDER BY avg_temperature DESC;


-- Section 17: Temperature difference from previous time
-- Calculate the temperature difference from the previous time for each location
SELECT
    lat,
    lon,
    date_and_time,
    temperature,
    ROUND(temperature - LAG(temperature) OVER (PARTITION BY lat, lon ORDER BY date_and_time), 2) AS temperature_difference
FROM Portfolio_project..temperature_france
ORDER BY 1, 2;


-- Section 18: Average mean sea level pressure ranking
-- Calculate the average mean sea level pressure for each location within a specific date range and rank them from lowest to highest
SELECT
    city,
    ROUND(AVG(mean_sea_level_pressure), 2) AS avg_mean_sea_level_pressure,
    RANK() OVER (ORDER BY AVG(mean_sea_level_pressure) DESC) AS mean_sea_level_pressure_rank
FROM Portfolio_project..weather_france wf  
WHERE date_and_time BETWEEN '2024-01-20 12:00' AND '2024-01-22 12:00'
GROUP BY city
ORDER BY avg_mean_sea_level_pressure;


-- Section 19: Running total precipitation
-- Calculate the running total of precipitation accumulated over the past 24 hours for each geographic coordinate
SELECT
    coordinates,
    date_and_time,
    precipitation_24_hours,
    SUM(precipitation_24_hours) OVER (PARTITION BY coordinates ORDER BY date_and_time) AS running_total_precipitation
FROM Portfolio_project..weather_france 
WHERE date_and_time BETWEEN '2024-01-16 00:00' AND '2024-01-18 00:00';


-- Section 20: Average precipitation over past 24 hours
-- Calculate the average precipitation accumulated over the past 24 hours for each day within a specific date range
SELECT
    FORMAT(CONVERT(datetime, date_and_time), 'yyyy-MM-dd') AS date,
    ROUND(AVG(precipitation_24_hours), 2) AS avg_precipitation_24_hours
FROM Portfolio_project..weather_france wf 
WHERE date_and_time BETWEEN '2024-01-20' AND '2024-01-25'
GROUP BY FORMAT(CONVERT(datetime, date_and_time), 'yyyy-MM-dd')
ORDER BY date;


-- Section 21: Maximum wind speed ranking
-- Identify the maximum wind speeds recorded for each geographic coordinate and rank them from highest to lowest
SELECT
    coordinates,
    MAX(wind_speed) AS max_wind_speed,
    RANK() OVER (ORDER BY MAX(wind_speed) DESC) AS wind_speed_rank
FROM Portfolio_project..weather_france wf 
GROUP BY coordinates
ORDER BY max_wind_speed DESC;


-- Section 22: Moving average of wind direction
-- Calculate the moving average of wind_direction for each location over a 3-day window
SELECT
    coordinates,
    CAST(date_and_time AS DATE) as date,
    wind_direction,
    ROUND(AVG(wind_direction) OVER (PARTITION BY coordinates ORDER BY date_and_time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 2) AS moving_avg_wind_direction
FROM Portfolio_project..weather_france wf;


-- Section 23: Minimum temperature, wind speed, and UV index
-- Identify the minimum temperature, minimum wind speed, and UV index for each unique combination of coordinates and UV index
SELECT 
    wf.coordinates,  
    wf.uv_index,
    MIN(tf.min_temperature) AS global_min_temperature, 
    MIN(wf.wind_speed) as min_wind_speed	
FROM Portfolio_project..weather_france wf
JOIN Portfolio_project..temperature_france tf 
	ON tf.date_and_time = wf.date_and_time
	AND tf.coordinates = wf.coordinates
GROUP BY wf.coordinates, wf.uv_index
ORDER BY wf.coordinates;


-- Section 24: Statistical summaries for wind speed by month
-- Provide statistical summaries, including average, minimum, maximum, variance, and standard deviation, for wind speed for each hour 
SELECT
    FORMAT(CONVERT(datetime, date_and_time), 'HH') AS hour,
    ROUND(MIN(wind_speed), 2) AS min_wind_speed, 
    ROUND(MAX(wind_speed), 2) AS max_wind_speed,
    ROUND(VAR(wind_speed), 2) AS var_wind_speed,
    ROUND(STDEV(wind_speed), 2) AS stdev_wind_speed
FROM Portfolio_project..weather_france wf
GROUP BY FORMAT(CONVERT(datetime, date_and_time), 'HH')
ORDER BY hour;


-- Section 25: Creation and usage of a view
-- Create a view, combine temperature and weather data, and filter temperatures above a defined threshold
CREATE VIEW AggregatedWeather AS
SELECT
    tf.lat,
    tf.lon,
    CONVERT(DATE, tf.date_and_time) AS extracted_date,
    ROUND(AVG(tf.temperature), 2) AS avg_temperature,
    MAX(wf.wind_speed) AS max_wind_speed
FROM
    Portfolio_project..temperature_france tf
JOIN
    Portfolio_project..weather_france wf
ON
    tf.lat = wf.lat
    AND tf.lon = wf.lon
    AND CONVERT(DATE, tf.date_and_time) = CONVERT(DATE, wf.date_and_time)
WHERE
    tf.temperature > 5
GROUP BY
    tf.lat,
    tf.lon,
    CONVERT(DATE, tf.date_and_time);

-- Select all columns from the AggregatedWeather for a specific date
SELECT *
FROM AggregatedWeather
WHERE extracted_date = '2024-01-18';


-- Section 26: Records count for each latitude range
-- Count the number of records for each latitude range
SELECT
    CASE
        WHEN lat BETWEEN 43 AND 44 THEN '43-44'
        WHEN lat BETWEEN 45 AND 46 THEN '45-46'
        -- Add more cases as needed
        ELSE 'Other'
    END AS latitude_range,
    COUNT(*) AS records_count
FROM Portfolio_project..temperature_france
GROUP BY lat
ORDER BY latitude_range;


-- Section 27: Wind speed and precipitation threshold analysis
-- Analyse wind speed and precipitation for specific date ranges, as well as specific wind speed threshold and precipitation threshold
SELECT
    FORMAT(CONVERT(datetime, date_and_time), 'yyyy_MM_dd') as date,
    ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
    ROUND(AVG(precipitation_24_hours), 2) AS avg_precipitation
FROM Portfolio_project..weather_france wf 
GROUP BY FORMAT(CONVERT(datetime, date_and_time), 'yyyy_MM_dd')
HAVING AVG(wind_speed) > 2 
       OR AVG(precipitation_24_hours) > 0.5;  


-- Section 28: Average wind speed at sunrise and sunset
-- Calculate the average wind speed during sunrise
SELECT
    FORMAT(sunrise, 'HH:mm') AS sunrise_time,
    ROUND(AVG(wind_speed), 2) AS avg_wind_speed_at_sunrise
FROM Portfolio_project..weather_france wf 
GROUP BY FORMAT(sunrise, 'HH:mm')
ORDER BY avg_wind_speed_at_sunrise DESC;

-- Calculate the average wind speed during sunset
SELECT
    FORMAT(CONVERT(datetime, sunset), 'HH:mm') AS sunset_time,
    ROUND(AVG(wind_speed), 2) AS avg_wind_speed_at_sunset
FROM Portfolio_project..weather_france wf 
GROUP BY FORMAT(CONVERT(datetime, sunset), 'HH:mm')
ORDER BY avg_wind_speed_at_sunset DESC;


-- Section 29: Average sunrise and sunset with city and UV index variations
-- Calculate average sunrise and sunset, considering variations in city and UV index
SELECT
    city,
    uv_index,
    FORMAT(CAST(AVG(CAST(sunrise AS FLOAT)) AS DATETIME), 'yyyy-MM-dd HH:mm') AS average_sunrise,
    FORMAT(CAST(AVG(CAST(sunset AS FLOAT)) AS DATETIME), 'yyyy-MM-dd HH:mm') AS average_sunset
FROM Portfolio_project..weather_france
GROUP BY CUBE (city, uv_index);


-- Section 30: Comparative analysis of temperature and wind direction trends
-- Compare trends by date and time in temperature and wind direction
SELECT date_and_time 
FROM Portfolio_project..temperature_france
GROUP BY date_and_time
HAVING AVG(temperature) > (SELECT AVG(temperature) FROM Portfolio_project..temperature_france)
UNION 
SELECT date_and_time 
FROM Portfolio_project..weather_france
GROUP BY date_and_time
HAVING AVG(wind_direction) > (SELECT AVG(wind_direction) FROM Portfolio_project..weather_france)
ORDER BY date_and_time;


-- Section 31: Temperature records for specific weather conditions
-- Retrieve temperature data where corresponding weather records meet specific criteria; 
-- Here, I get an empty table since the specified conditions in the WHERE clause are not being met in the data
SELECT *
FROM Portfolio_project..temperature_france
WHERE EXISTS (
	SELECT *
	FROM Portfolio_project..weather_france 
	WHERE Portfolio_project..weather_france.wind_direction BETWEEN .55 AND .56
		AND Portfolio_project..weather_france.precipitation_24_hours = 0
);


