-- TODO: SELECT QUARTER
DECLARE QUERY_START_DATE DATE DEFAULT '2021-01-01';
DECLARE QUERY_END_DATE DATE DEFAULT '2021-04-01';

WITH
-- US counties/blockgroups are identified for test results using a GIS approach. The lat/lon
-- annotated on each test row is looked up in the polygons of counties/blockgroups provided
-- by Google Public Datasets 
census_boundaries AS (
  SELECT
    CAST(geo_id AS STRING) AS GEOID,
-- TODO: SELECT CENSUS boundaries/public dataset, and respective geom column
    county_geom AS WKT
  FROM `bigquery-public-data.geo_us_boundaries.counties`

--     blockgroup_geom AS WKT
--   FROM `bigquery-public-data.geo_census_blockgroups.us_blockgroups_national`
),
--*******************************************************************************************************************
--********************************************* START WITH UPLOAD TESTS *********************************************
--Select the initial set of tests
dl_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CASE WHEN node._instruments IN ("tcpinfo", "web100") 
      THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.region)
    WHEN node._instruments IN ("ndt7", "ndt5")
      THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.Subdivision1ISOCode) 
    END AS state,
    census_boundaries.GEOID AS GEOID,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT,
    a.LossRate AS LossRate
  FROM `measurement-lab.ndt.unified_downloads`, census_boundaries
  WHERE date BETWEEN QUERY_START_DATE AND QUERY_END_DATE
  AND client.Geo.CountryCode = "US"
  AND (client.Geo.Subdivision1ISOCode IS NOT NULL OR client.Geo.Region IS NOT NULL)
  AND (client.Geo.Subdivision1ISOCode != "" OR client.Geo.Region != "")
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), census_boundaries.WKT
  )  
  AND a.MeanThroughputMbps != 0
),
--Filter for only tests With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT * FROM dl_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND state IS NOT NULL AND state != ""
    AND GEOID IS NOT NULL AND GEOID != ""
    AND ip IS NOT NULL
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order
dl_fingerprinted AS (
  SELECT
    date,
    continent_code,
    country_code,
    state,
    GEOID,
    ip,
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT, LossRate) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, state, GEOID, ip
),
--Select a random row for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in `dl_stats_per_day` 
dl_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    state,
    GEOID,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1
  FROM dl_fingerprinted
),
--Calculate statistics per day from random samples
dl_stats_per_day AS (
  SELECT
    date, continent_code, country_code, state, GEOID,
    COUNT(*) AS dl_samples_day,
    ROUND(MIN(random1.mbps),3) AS download_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS download_MED,
    ROUND(AVG(random1.mbps),3) AS download_AVG,
    ROUND(MAX(random1.mbps),3) AS download_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS download_minRTT_MED,
    ROUND(APPROX_QUANTILES(random1.LossRate, 100) [SAFE_ORDINAL(50)],3) AS download_LossRate_MED,
  FROM dl_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, state, GEOID
),
--*******************************************************************************************************************
--********************************************* REPEAT FOR UPLOAD TESTS ********************************************* 
--Select the initial set of tests
ul_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CASE WHEN node._instruments IN ("tcpinfo", "web100") 
      THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.region)
    WHEN node._instruments IN ("ndt7", "ndt5")
      THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.Subdivision1ISOCode) END AS state,
    census_boundaries.GEOID AS GEOID,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps
  FROM `measurement-lab.ndt.unified_uploads`, census_boundaries
  WHERE date BETWEEN QUERY_START_DATE AND QUERY_END_DATE
  AND client.Geo.CountryCode = "US"
  AND (client.Geo.Subdivision1ISOCode IS NOT NULL OR client.Geo.Region IS NOT NULL)
  AND (client.Geo.Subdivision1ISOCode != "" OR client.Geo.Region != "")
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), census_boundaries.WKT
  )  
  AND a.MeanThroughputMbps != 0),
--Filter for only tests With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT * FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND state IS NOT NULL AND state != ""
    AND GEOID IS NOT NULL AND GEOID != ""
    AND ip IS NOT NULL
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order.
ul_fingerprinted AS (
  SELECT
    date,
    continent_code,
    country_code,
    state,
    GEOID,
    ip,
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, country_code, state, GEOID, ip
),
-- Select a row for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in `ul_stats_per_day` 
ul_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    state,
    GEOID,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1
  FROM ul_fingerprinted
),
--Calculate statistics per day from random samples
ul_stats_per_day AS (
  SELECT
    date, continent_code, country_code, state, GEOID,
    COUNT(*) AS ul_samples_day,
    ROUND(MIN(random1.mbps),3) AS upload_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS upload_MED,
    ROUND(AVG(random1.mbps),3) AS upload_AVG,
    ROUND(MAX(random1.mbps),3) AS upload_MAX
  FROM ul_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, state, GEOID
),
--*******************************************************************************************************************
--Gather final results **********************************************************************************************
results AS (
  SELECT * 
  FROM dl_stats_per_day 
  JOIN ul_stats_per_day USING (date, continent_code, country_code, state, GEOID)
)


--*******************************************************************************************************************
--Show final results ************************************************************************************************
SELECT  
  GEOID, 
  -- download
  ROUND(MIN(download_MIN),  2)    AS download_MIN,
  ROUND(AVG(download_MED),  2)    AS download_MED,
  ROUND(AVG(download_AVG),  2)    AS download_AVG,
  ROUND(MAX(download_MAX),  2)    AS download_MAX,
  -- upload
  ROUND(MIN(upload_MIN),    2)    AS upload_MIN,
  ROUND(AVG(upload_MED),    2)    AS upload_MED,
  ROUND(AVG(upload_AVG),    2)    AS upload_AVG, 
  ROUND(MAX(upload_MAX),    2)    AS upload_MAX,
  -- others
  ROUND(AVG(download_minRTT_MED),   2)   AS latency,
  ROUND(AVG(download_LossRate_MED), 4)   AS lossrate
FROM results
GROUP BY GEOID
ORDER BY GEOID