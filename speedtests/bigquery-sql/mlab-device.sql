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
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
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
--*******************************************************************************************************************
--Gather final results **********************************************************************************************
download_stats AS (
    SELECT
        GEOID,
        COUNT(*) AS num_test_down,
        COUNT(DISTINCT ip) AS num_device_down
    FROM dl_per_location_cleaned
    GROUP BY GEOID
),
upload_stats AS (
    SELECT
        GEOID,
        COUNT(*) AS num_test_up,
        COUNT(DISTINCT ip) AS num_device_up
    FROM ul_per_location_cleaned
    GROUP BY GEOID
)


--*******************************************************************************************************************
--Show final results ************************************************************************************************
SELECT  *
-- Here, use FULL OUTER JOIN to get num_test and num_device for each GEOID, regardless of upload/download mode
FROM download_stats FULL OUTER JOIN upload_stats USING (GEOID)
-- FROM download_stats JOIN upload_stats USING (GEOID)
ORDER BY GEOID