import sys
import geopandas as gp
import numpy as np
import pandas as pd

SF52 = { '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', 
                      '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', 
                      '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', 
                      '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', 
                      '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', 
                      '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', 
                      '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', 
                      '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', 
                      '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', 
                      '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', 
                      '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', 
                      '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', 
                      '54': 'WV', '55': 'WI', '56': 'WY', '72': 'PR'}
SF52R = {v: k for (k,v) in SF52.items()}

STATE_LENGTH, COUNTY_LENGTH, CT_LENGTH, CBG_LENGTH, CB_LENGTH = 2, 5, 11, 12, 15
CRS_TIGER, CRS_OOKLA = 4269, 4326

def aggregated_speed_ookla(ookla_tiles):
    # convert to Mbps for easier reading
    ookla_tiles['avg_d_mbps'] = ookla_tiles['avg_d_kbps'] / 1000
    ookla_tiles['avg_u_mbps'] = ookla_tiles['avg_u_kbps'] / 1000

    # RARE CASE: when devices > tests, maybe due to (1) Ookla's bad raw data, or (2) our Quantile-based Flooring and Capping process independently created this scenario
    # FIX: set devices = tests
    ookla_tiles['devices'] = np.where(ookla_tiles['devices'] > ookla_tiles['tests'], 
            ookla_tiles['tests'], ookla_tiles['devices'])

    # weighted average speed; NOTE the groupby column GEOID
    return (
        ookla_tiles.groupby("GEOID")
        .apply(
            lambda x: pd.Series(
                {
                    "meanDownloadMbpsOokla"   : np.average(x["avg_d_mbps"], weights=x["tests"]),
                    "meanUploadMbpsOokla"     : np.average(x["avg_u_mbps"], weights=x["tests"]),
                    "medDownloadMbpsOokla"    : np.median(x["avg_d_mbps"]),
                    "medUploadMbpsOokla"      : np.median(x["avg_u_mbps"]),
                    'maxDownloadMbpsOokla'    : np.max(x['avg_d_mbps']),
                    'maxUploadMbpsOokla'      : np.max(x['avg_u_mbps']),
                    'minDownloadMbpsOokla'    : np.min(x['avg_d_mbps']),
                    'minUploadMbpsOokla'      : np.min(x['avg_u_mbps']),
                    "latencyOokla"            : np.average(x["avg_lat_ms"], weights=x["tests"]),
                }
            )
        )
        .reset_index()
        .merge(
            ookla_tiles.groupby("GEOID")
            .agg(numTestOokla=("tests", "sum"), numDeviceOokla=("devices", "sum"))
            .reset_index(),
            on=["GEOID"],
        )
        .round(2) # round to 2 decimal places for easier reading
    )


abbrv = sys.argv[1].lower()
ABBRV = abbrv.upper()
sf = SF52R[ABBRV]

ookla_tiles_dir = 'ookla_state_tiles/'
census_year = '2019'

# REPEAT process for each quarter, as needed
for QUARTER in ['2021Q1', '2021Q2', '2021Q3', '2021Q4']:
    ookla_state_tiles = gp.read_file(f"{ookla_tiles_dir}{QUARTER}_state_{sf}.geojson")
    
    # PART A: CLEANING DATA

    # All test instances (for Ookla, an instance is a test-aggregated quad-tile) where any round trip time (average latency for all tests within quad-tile) was reported as 0.5ms or lower were removed.
    # All test instances where the range of a unit’s of individual round trip times exceeded 300ms were removed.
    ookla_state_tiles = ookla_state_tiles[
        (ookla_state_tiles.avg_lat_ms > 0.5) & (ookla_state_tiles.avg_lat_ms <= 300)]
    # NOT APPLICABLE: All test instances where a unit’s packet loss exceeded 10% within a single hour were removed. 

    # Quantile-based Flooring and Capping
    for col in ['avg_d_kbps', 'avg_u_kbps', 'avg_lat_ms', 'devices', 'tests']:
        floor_rate = 0.005
        # set higher floor_rate for these values (due to non-uniform distributions of tests and latency)
        if col in ['avg_lat_ms', 'devices', 'tests']: 
            floor_rate = 0.10
        cap_rate = 1 - floor_rate
        floor_value = ookla_state_tiles[col].quantile(floor_rate) 
        cap_value = ookla_state_tiles[col].quantile(cap_rate)
        
        # # Show the number of rows floored/capped
        # print(floor_value, cap_value, ookla_state_tiles.shape)
        # print(ookla_state_tiles[ookla_state_tiles[col] < floor_value].shape)
        # print(ookla_state_tiles[ookla_state_tiles[col] > cap_value].shape)

        # Skewness value explains the extent to which the data is normally distributed. 
        # Ideally between -1 and +1), any deviation from this range indicates the presence of extreme values. 
        # e.g. The skewness value of 6.5 (right-skewed) shows that the variable has extreme higher values. 
        # print("*** COL = ", col, ", initial skew: ", ookla_state_tiles[col].skew())

        ookla_state_tiles[col] = np.where(
            ookla_state_tiles[col] < floor_value, floor_value, ookla_state_tiles[col])
        ookla_state_tiles[col] = np.where(
            ookla_state_tiles[col] > cap_value, cap_value, ookla_state_tiles[col])
        # print("Improved skew: ", ookla_state_tiles[col].skew())
    
    
    
    # PART B: Generate ookla (by state) CBG and CB tiles
    for census_level in ['CBG', 'CB']:
        if census_level == 'CBG':
            state_shp_url = f"https://www2.census.gov/geo/tiger/TIGER{census_year}/BG/tl_{census_year}_{sf}_bg.zip"
            state_census_tiles = gp.read_file(state_shp_url).to_crs(CRS_OOKLA)
        else:
            state_shp_url = f"https://www2.census.gov/geo/tiger/TIGER2019/TABBLOCK/tl_2019_{sf}_tabblock10.zip"
            state_census_tiles = gp.read_file(state_shp_url).to_crs(CRS_OOKLA)
            state_census_tiles = state_census_tiles.rename(columns={"GEOID10": "GEOID"})

        # Joining ookla tiles with census tiles
        joined_tiles = gp.sjoin(ookla_state_tiles, state_census_tiles, how="inner", predicate='intersects')
        
        if len(joined_tiles) == 0:
            print(f"ALERT: {QUARTER}_{census_level}_{sf}: UNLIKELY EVENT: joined_tiles contain no ookla records")
        else:
            # Weight test results. 
            weighted_df = aggregated_speed_ookla(joined_tiles)
            # add speed source
            if census_level == 'CBG':
                # RENAME the GEOID column to match ES mapping
                weighted_df = weighted_df.rename(columns={"GEOID": "GEOID_cbg"})
                weighted_df['speedSourceOokla'] = 'interpolatedFromTilesCBG'
            else:
                weighted_df['speedSourceOokla'] = 'joinedTilesAtCB'

            saved_file = f'{ookla_tiles_dir}{QUARTER}_{census_level}_{sf}.csv'
            weighted_df.to_csv(saved_file, index=False)
            print(f"{SF52[sf]} {sf=}: Joined and saved {weighted_df.shape} to {saved_file}")
