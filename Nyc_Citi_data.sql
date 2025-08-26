-- Creating the database
CREATE DATABASE IF NOT EXISTS nyc_citi_bike;

USE nyc_citi_bike;

-- Creating the table structure
CREATE TABLE nyc_citi_bike_raw_data
(
Start_date VARCHAR(255),
Stop_date VARCHAR(255),
StartStation_id INT,
StartStation_name VARCHAR(255),
EndStation_id INT,
EndStation_name VARCHAR(255),
Bike_id INT,
UserType VARCHAR(255),
BirthYear INT,
Age INT,
AgeGroup Varchar(255),
TripDuration_sec INT,
TripDuration_min INT,
`Month` INT,
Season VARCHAR(255),
Temperature INT,
Weekday VARCHAR(255)
);

SELECT *
FROM nyc_citi_bike_raw_data;

DROP TABLE nyc_citi_bike_raw_data;

LOAD DATA INFILE 'nyc_citi_bike_raw_data.csv' INTO TABLE nyc_citi_bike_raw_data
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

SELECT *
FROM nyc_citi_bike_raw_data;

-- How many rows are in the table (16844)
SELECT COUNT(*)
FROM nyc_citi_bike_raw_data;

-- Table structure
DESCRIBE nyc_citi_bike_raw_data;

-- Check for NULLS in columns
SELECT
	SUM(CASE WHEN Start_date IS NULL THEN 1 ELSE 0 END) AS null_start_date,
	SUM(CASE WHEN BirthYear IS NULL THEN 1 ELSE 0 END) AS null_birth_year,
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS null_age
FROM nyc_citi_bike_raw_data;

-- Check for unique values
SELECT DISTINCT UserType
FROM nyc_citi_bike_raw_data;

-- Check for invalid ages
SELECT *
FROM nyc_citi_bike_raw_data
WHERE Age < 5 OR Age > 100;

-- Convert date strings to datetime
	-- Creating new columns
ALTER TABLE nyc_citi_bike_raw_data
ADD COLUMN Start_datetime DATETIME,
ADD COLUMN Stop_datetime DATETIME;

	-- Update the columns with new values
UPDATE nyc_citi_bike_raw_data
SET Start_datetime = STR_TO_DATE(TRIM(REPLACE(Start_date, '  ', ' ')), '%d/%m/%Y %H:%i'),
    Stop_datetime = STR_TO_DATE(TRIM(REPLACE(Stop_date, '  ', ' ')), '%d/%m/%Y %H:%i');
    
	-- Drop the old columns (staging data, not original raw data)
ALTER TABLE nyc_citi_bike_raw_data
DROP COLUMN Start_date,
DROP COLUMN Stop_date;

	-- Add hour, time of day
ALTER TABLE nyc_citi_bike_raw_data
ADD COLUMN Hour_of_day INT,
ADD COLUMN Time_of_day VARCHAR(30);

	-- Fill them with values
UPDATE nyc_citi_bike_raw_data
SET Hour_of_day = HOUR(Start_datetime),
	Time_of_day = CASE
		WHEN HOUR(Start_datetime) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN HOUR(Start_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN HOUR(Start_datetime) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
	END;
    
    
-- Check top stations to start trip
SELECT StartStation_name, COUNT(*) AS trip_count
FROM nyc_citi_bike_raw_data
GROUP BY StartStation_name
ORDER BY trip_count DESC;

-- Check top stations to end trip
SELECT EndStation_name, COUNT(*) AS endTrip_count
FROM nyc_citi_bike_raw_data
GROUP BY EndStation_name
ORDER BY endTrip_count DESC;

-- Compare stations based on start, end trip
SELECT
	COALESCE(start.StartStation_name, end.EndStation_name) AS Station,
    IFNULL(start.trip_count, 0) AS Start_Trips,
    IFNULL(end.endTrip_count, 0) AS End_Trips
FROM
	(SELECT StartStation_name, COUNT(*) AS trip_count
    FROM nyc_citi_bike_raw_data
    GROUP BY StartStation_name) AS start
LEFT JOIN
	(SELECT EndStation_name, COUNT(*) AS endTrip_count
    FROM nyc_citi_bike_raw_data
    GROUP BY EndStation_name) AS end
ON start.StartStation_name = end.EndStation_name

UNION

SELECT
    COALESCE(start.StartStation_name, end.EndStation_name) AS Station,
    IFNULL(start.trip_count, 0) AS Start_Trips,
    IFNULL(end.endTrip_count, 0) AS End_Trips
FROM
    (SELECT StartStation_name, COUNT(*) AS trip_count
     FROM nyc_citi_bike_raw_data
     GROUP BY StartStation_name) AS start
RIGHT JOIN
    (SELECT EndStation_name, COUNT(*) AS endTrip_count
     FROM nyc_citi_bike_raw_data
     GROUP BY EndStation_name) AS end
ON start.StartStation_name = end.EndStation_name;



-- Trips per Month
SELECT `Month`, COUNT(*) AS trips
FROM nyc_citi_bike_raw_data
GROUP BY `Month`
ORDER BY `Month`;


-- Weekday trends
SELECT Weekday, COUNT(*) AS trip_count
FROM nyc_citi_bike_raw_data
GROUP BY Weekday
ORDER BY FIELD(Weekday, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


-- Subscriber vs One-time user
SELECT UserType, COUNT(*) AS trip_count, AVG(TripDuration_min) AS avg_duration
FROM nyc_citi_bike_raw_data
GROUP BY UserType;

-- Age and duration connection (anomalies at age 26 and 80)
SELECT Age, AVG(TripDuration_min) AS avg_duration
FROM nyc_citi_bike_raw_data
GROUP BY Age
ORDER BY Age;

-- Searching for more anomaly
SELECT *
FROM nyc_citi_bike_raw_data
ORDER BY TripDuration_min DESC
LIMIT 10;

-- Flag the outlier datas
ALTER TABLE nyc_citi_bike_raw_data ADD COLUMN Is_anomaly BOOLEAN;

UPDATE nyc_citi_bike_raw_data
SET Is_anomaly = TripDuration_min >= 240;

SELECT *
FROM nyc_citi_bike_raw_data;

-- Create a stations table with IDs
CREATE TABLE stations AS
SELECT DISTINCT StartStation_id AS station_id, StartStation_name AS station_name
FROM nyc_citi_bike_raw_data

UNION

SELECT DISTINCT EndStation_id AS station_id, EndStation_name AS station_name
FROM nyc_citi_bike_raw_data;

SELECT *
FROM stations;

