import geopandas as gp
import numpy as np
import pandas as pd
from glob import glob
import datetime
import os



# FIND all counties/filename_stem/zipped json files associated with these states
# All geojson.zip files are listed, exlude counties that are not found in verse.csv, or US terriories! 
# i.e. only present and valid stems!
# For UNZIPPING command
# For parcel_parse.py command
STATES = ['fl', 'md', 'in', 'il', 'oh', 'wa' , 'tx']
updated_counties_file = "temp_updated_counties.txt"
updated_zips_file = "temp_updated_zips.txt"


REGRID_BUCKET_DIR = '/home/nhat/regrid-bucket/'
 

all_bucket_jsons = glob(f"{REGRID_BUCKET_DIR}/*.json")
all_bucket_filename_stem = set(os.path.basename(path).split('.')[0] for path in all_bucket_jsons)


verse = gp.read_file(f'{REGRID_BUCKET_DIR}verse.csv.zip')
verse = verse.replace('', np.nan)
print(f"{verse.shape=}")
all_valid_filename_stem = set(verse.filename_stem)
    
# Find and save new changes to files 
# date_{last_refresh}, updated_counties.txt, updated_states.txt, updated_zips.txt
# mbtiles_update_cmds.txt, postgis_update_cmds.txt
    
# can drop/ignore counties without last_refresh values
verse_refresh = verse.dropna(subset=['last_refresh'])[['county', 'state', 'geoid', 'last_refresh', 'filename_stem']].copy()
print(f"{verse_refresh.shape=}")

ver = verse_refresh.reset_index(drop=True)
# EXCLUDE us territories
us_territories_noncoverage= {'vi_st-john', 'gu_guam', 'vi_st-thomas', 'mp_tinian', 'mp_rota', 'vi_st-croix', 'mp_saipan'}
ver = ver[~ver['county'].isin(us_territories_noncoverage)]
# last_refresh has datetime.date type
ver['last_refresh'] = pd.to_datetime(ver['last_refresh'], format='%Y-%m-%d').dt.date
print(f"Most recent refresh dates: {ver.sort_values('last_refresh').groupby('last_refresh').size().tail()}")



updated_counties = []
updated_zips = []

for abbrv in STATES:
    abbrv = abbrv.upper()
    
    all_state_stems = ver.loc[ver.state == abbrv]
    # # ensure the files are presented in geobucket
    state_updated_counties = set(all_state_stems.filename_stem).intersection(all_bucket_filename_stem)
    updated_counties.extend(state_updated_counties)
    updated_zips.extend([f"{REGRID_BUCKET_DIR}{name}.geojson.zip" for name in state_updated_counties])

    
pd.DataFrame(sorted(updated_counties)).to_csv(updated_counties_file, index=False, header=False, sep='\n')
pd.DataFrame(sorted(updated_zips)).to_csv(updated_zips_file, index=False, header=False, sep='\n')

len(updated_counties)

