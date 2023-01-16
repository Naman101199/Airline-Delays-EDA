# Select database
USE flights_db;

# List of all tables in the database
SHOW TABLES;

# Select statments for all the tables
SELECT * FROM airline;
SELECT * FROM airport;
SELECT * FROM location;
SELECT * FROM weather;
SELECT * FROM aircraft;
SELECT * FROM flight;

##CASE 1
/*------ Arrival delay due to departure delay at Destination grouped by airline ----*/
SELECT 
    ap.AIRPORT_NAME AS DEST_AIRPORT,
    a.UNIQUE_CARRIER_NAME AS AIRLINE,
    COUNT(*) AS DELAY_COUNT
FROM
    flight f
        INNER JOIN
    aircraft af ON f.AIRCRAFT_ID = af.AIRCRAFT_ID
        INNER JOIN
    airline a ON a.AIRLINE_ID = af.AIRLINE_ID
        INNER JOIN
    airport ap ON f.DEST_AIRPORT_ID = ap.AIRPORT_ID
WHERE
    f.CRS_DEP_TIME < f.DEP_TIME
        AND f.CRS_ARR_TIME < f.ARR_TIME
GROUP BY ap.AIRPORT_NAME , a.UNIQUE_CARRIER_NAME
ORDER BY DEST_AIRPORT , DELAY_COUNT DESC;

##CASE 2
/*------ Average delay at Origin Airport-----*/
SELECT 
    ap.AIRPORT_NAME as ORIGIN_AIRPORT,
    ROUND(AVG(MINUTE(TIMEDIFF(f.DEP_TIME, f.CRS_DEP_TIME)) + HOUR(TIMEDIFF(f.DEP_TIME, f.CRS_DEP_TIME)) * 60)) AS DELAY_AT_ORIGIN
FROM
    flight f
        INNER JOIN
    aircraft af ON f.AIRCRAFT_ID = af.AIRCRAFT_ID
        INNER JOIN
    airline a ON a.AIRLINE_ID = af.AIRLINE_ID
        INNER JOIN
    airport ap ON f.ORIGIN_AIRPORT_ID = ap.AIRPORT_ID
WHERE
    f.CRS_DEP_TIME < f.DEP_TIME
GROUP BY ap.AIRPORT_NAME
ORDER BY DELAY_AT_ORIGIN DESC;

##CASE 3
/*--- % Monthly cancellations due to fog by airport ---*/
SELECT 
    a.AIRPORT_NAME AS ORIGIN_AIRPORT,
    MONTH(f.DATE) AS MONTH,
    SUM(CASE WT01
        WHEN 1 THEN 1
        ELSE 0
    END) CANCELLATION_DUE_TO_FOG,
    COUNT(*) AS CANCELLED_COUNT,
    ROUND(SUM(CASE WT01
                WHEN 1 THEN 1
                ELSE 0
            END) / COUNT(*) * 100,
            2) AS PERCENT_CANCELLATIONS
FROM
    flight f
        INNER JOIN
    weather w ON f.ORIGIN_AIRPORT_ID = w.AIRPORT_ID
        AND f.DATE = w.DATE
        INNER JOIN
    airport a ON a.airport_id = f.ORIGIN_AIRPORT_ID
WHERE
    f.CANCELLED = 1 AND YEAR(f.DATE) = 2019
GROUP BY a.AIRPORT_NAME , MONTH(f.DATE)
ORDER BY CANCELLED_COUNT DESC;


##CASE 4
/*------ Arrival delay count by city and state ----*/
SELECT 
    l.CITY, l.STATE, COUNT(*) AS DELAY_COUNT
FROM
    flight f
        INNER JOIN
    airport ap ON f.DEST_AIRPORT_ID = ap.AIRPORT_ID
        INNER JOIN
    location l ON ap.LOCATION_ID = l.LOCATION_ID
WHERE
    f.CRS_ARR_TIME < f.ARR_TIME
GROUP BY l.CITY , l.STATE
HAVING DELAY_COUNT > 999
ORDER BY DELAY_COUNT DESC;


##CASE 5
/*------ Avg Arrival delay in Mins due to fog grouped by airports and airline ----*/
SELECT 
    ap.AIRPORT_NAME,
    a.UNIQUE_CARRIER_NAME,
    ROUND(AVG(MINUTE(TIMEDIFF(f.ARR_TIME, f.CRS_ARR_TIME)) + HOUR(TIMEDIFF(f.ARR_TIME, f.CRS_ARR_TIME)) * 60)) DELAY_IN_MINS
FROM
    airport ap
        INNER JOIN
    weather w ON ap.AIRPORT_ID = w.AIRPORT_ID
        INNER JOIN
    (SELECT 
        *
    FROM
        flight
    WHERE
        CRS_DEP_TIME < DEP_TIME
            AND CRS_ARR_TIME < ARR_TIME) f ON ap.AIRPORT_ID = f.DEST_AIRPORT_ID
        AND f.DATE = w.DATE
        INNER JOIN
    aircraft af ON f.AIRCRAFT_ID = af.AIRCRAFT_ID
        INNER JOIN
    airline a ON a.AIRLINE_ID = af.AIRLINE_ID
WHERE
    WT01 = 1
GROUP BY ap.AIRPORT_NAME , a.UNIQUE_CARRIER_NAME
HAVING DELAY_IN_MINS > 30
ORDER BY DELAY_IN_MINS DESC;

##CASE 6
/*------ Arrival Delay count due to heavy Snow ----*/
SELECT 
    airport_name AS DEST_AIRPORT,
    COUNT(*) DELAY_COUNT_DUE_TO_SNOW
FROM
    flight f
        JOIN
    (SELECT 
        date, airport_id, snow
    FROM
        weather
    WHERE
        COALESCE(snow, 0) > 3) w ON f.dest_airport_id = w.airport_id
        AND f.date = w.date
        JOIN
    airport a ON f.dest_airport_id = a.airport_id
WHERE
    crs_dep_time < dep_time
        OR crs_arr_time < arr_time
GROUP BY DEST_AIRPORT
ORDER BY DELAY_COUNT_DUE_TO_SNOW DESC;

##CASE 7
/*------ Details of 5 most delayed flights ----*/
SELECT 
    a.UNIQUE_CARRIER_NAME AS CARRIER,
    ORIGIN_AIRPORT_NAME,
    DEST_AIRPORT_NAME,
    ORIGIN_CITY,
    ORIGIN_STATE,
    DEST_CITY,
    DEST_STATE,
    DATE,
    f.CRS_DEP_TIME,
    f.DEP_TIME,
    f.CRS_ARR_TIME,
    f.ARR_TIME,
    TIMEDIFF(f.ARR_TIME, f.CRS_ARR_TIME) AS DELAY_DURATION
FROM
    flight f
        INNER JOIN
    (SELECT 
        ap.AIRPORT_ID AS DEST_AIRPORT_ID,
            ap.AIRPORT_NAME AS DEST_AIRPORT_NAME,
            l.CITY AS DEST_CITY,
            l.STATE AS DEST_STATE
    FROM
        airport ap
    INNER JOIN location l ON ap.LOCATION_ID = l.LOCATION_id) a1 ON f.DEST_AIRPORT_ID = a1.DEST_AIRPORT_ID
        INNER JOIN
    (SELECT 
        ap.AIRPORT_ID AS ORIGIN_AIRPORT_ID,
            ap.AIRPORT_NAME AS ORIGIN_AIRPORT_NAME,
            l.CITY AS ORIGIN_CITY,
            l.STATE AS ORIGIN_STATE
    FROM
        airport ap
    INNER JOIN location l ON ap.LOCATION_ID = l.LOCATION_id) a2 ON f.ORIGIN_AIRPORT_ID = a2.ORIGIN_AIRPORT_ID
        INNER JOIN
    aircraft af ON f.AIRCRAFT_ID = af.AIRCRAFT_ID
        INNER JOIN
    airline a ON a.AIRLINE_ID = af.AIRLINE_ID
WHERE
    f.crs_arr_time < f.arr_time
ORDER BY DELAY_DURATION DESC
LIMIT 5;


