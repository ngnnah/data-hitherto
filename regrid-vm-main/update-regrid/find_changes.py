import geopandas as gp
import numpy as np
import pandas as pd
from glob import glob
import datetime
import os

today = datetime.datetime.today().date()
print("==========================================")
print(f"FINDING CHANGES, start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")


pg_driver = 'postgresql+psycopg2'
pg_user = 'boss_user'
pg_pass = 'passDEV9g47uibjn2ijovZ'
pg_host = 'dev-regrid.db.ready.net'
pg_port = '5432'
pg_db = 'boss_db_dev'
# import sqlalchemy
# import geoalchemy2
# engine = sqlalchemy.create_engine(f"{pg_driver}://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
REGRID_BUCKET_DIR = '/home/nhat/regrid-bucket/'

updated_states_file = "updated_states.txt"
updated_counties_file = "updated_counties.txt"
updated_zips_file = "updated_zips.txt"
mbtiles_update_cmds_file = 'mbtiles_update_cmds.txt'
postgis_update_cmds_file = 'postgis_update_cmds.txt'
postgis_delete_cmds_file = 'postgis_delete_cmds.txt'

all_bucket_jsons = glob(f"{REGRID_BUCKET_DIR}/*.json")
all_bucket_filename_stem = set(os.path.basename(path).split('.')[0] for path in all_bucket_jsons)


verse = gp.read_file(f'{REGRID_BUCKET_DIR}verse.geojson.zip')
verse = verse.replace('', np.nan)
print(f"{verse.shape=}")
all_valid_filename_stem = set(verse.filename_stem)
    
# Find and save new changes to files 
# date_{last_refresh}, updated_counties.txt, updated_states.txt, updated_zips.txt
# mbtiles_update_cmds.txt, postgis_update_cmds.txt
def main():
    
    # can drop/ignore counties without last_refresh values
    verse_refresh = verse.dropna(subset=['last_refresh'])[['county', 'state', 'geoid', 'last_refresh', 'filename_stem']].copy()
    print(f"{verse_refresh.shape=}")

    ver = verse_refresh.reset_index(drop=True)
    
    # # BEFORE: exclude us territories
    # us_territories_noncoverage= {'vi_st-john', 'gu_guam', 'vi_st-thomas', 'mp_tinian', 'mp_rota', 'vi_st-croix', 'mp_saipan'}
    # ver = ver[~ver['county'].isin(us_territories_noncoverage)]
    
    # last_refresh has datetime.date type
    ver['last_refresh'] = pd.to_datetime(ver['last_refresh'], format='%Y-%m-%d').dt.date
    print(f"Most recent refresh dates: {ver.sort_values('last_refresh').groupby('last_refresh').size().tail()}")
    latest_refresh_date = ver['last_refresh'].max()
    print(f"{latest_refresh_date=}")

    last_update_date = sorted(glob("date_*"))[-1].split('_')[1]
    last_update_date = datetime.datetime.fromisoformat(last_update_date).date()
    print(f"{last_update_date=}")

    
    # UPDATE condition
    if latest_refresh_date > last_update_date:
        # find counties that are refreshed after last_update_date
        new_counties = ver.loc[ver['last_refresh'] > last_update_date]
        
        updated_states = sorted(new_counties.state.str.lower().unique()) 
        # ensure the files are presented in geobucket
        updated_counties = sorted(set(new_counties.filename_stem).intersection(all_bucket_filename_stem))
        updated_zips = [f"{REGRID_BUCKET_DIR}{name}.geojson.zip" for name in updated_counties]
        
        print(f"{len(updated_states)} states -- {len(updated_counties)} counties are recently refreshed")
        # input files for next steps
        pd.DataFrame(updated_states).to_csv(updated_states_file, index=False, header=False, sep='\n')
        pd.DataFrame(updated_counties).to_csv(updated_counties_file, index=False, header=False, sep='\n')
        pd.DataFrame(updated_zips).to_csv(updated_zips_file, index=False, header=False, sep='\n')
        # save logs
        pd.DataFrame(updated_states).to_csv(f"./logs/{latest_refresh_date}_{updated_states_file}", index=False, header=False, sep='\n')
        pd.DataFrame(updated_counties).to_csv(f"./logs/{latest_refresh_date}_{updated_counties_file}", index=False, header=False, sep='\n')
        pd.DataFrame(updated_zips).to_csv(f"./logs/{latest_refresh_date}_{updated_zips_file}", index=False, header=False, sep='\n')

        # GENERATE mbtiles tiling cmds
        gen_tippecanoe_commands(updated_states, latest_refresh_date)
        
        # GENERATE ogr2ogr upload to postgis cmds
        gen_postgis_update_cmds(updated_counties, latest_refresh_date)
        
        # FINISH UPDATING; log this update by creating a new date_{last_update_date} file
        open(f"date_{latest_refresh_date}", 'w').close()
        
    else:
        # day after the update day: truncate input sources so cron tasks thereafter dont redo duplicate works
        print(f"NO REGRID REFRESHES FOUND, current time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
        open(updated_states_file, 'w').close()
        open(updated_counties_file, 'w').close()
        open(updated_zips_file, 'w').close()
        open(mbtiles_update_cmds_file, 'w').close()
        open(postgis_update_cmds_file, 'w').close()
        open(postgis_delete_cmds_file, 'w').close()

        
    print(f"SCRIPT find_changes.py completed, finish time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
    print("==========================================")
    return



# GENERATE ogr2ogr upload to postgis cmds
def gen_postgis_update_cmds(updated_counties, latest_refresh_date):
    print("Starting gen_postgis_update_cmds")
    # field "wkb_geometry", while using ogr2ogr, depends on -dialect flag ( _ogr_geometry_ or GEOMETRY sqlite)  
    # i.e. ogr2ogr will skip/raise error if try to manuall add this column; should build new tables on public.template table
    sql_select_fields = "geoid,ll_uuid,parcelnumb,census_blockgroup,lat,lon,numunits,owntype,owner,"
    sql_select_fields += "usedesc,zoning,mailadd,address,address2,sunit,scity,szip,"
    sql_select_fields += "usps_vacancy,lbcs_function,lbcs_function_desc,lbcs_structure,lbcs_structure_desc,lbcs_activity,lbcs_activity_desc,lbcs_site,lbcs_site_desc,"
    sql_select_fields += "rdi,dpv_status,dpv_codes,dpv_notes,dpv_type,ll_bldg_count,ll_bldg_footprint_sqft"
    connString = f"PG:'host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}'"
    
    uploadCmds = []
    deleteCmds = []
    
    for filename_stem in updated_counties:
        abbrv = filename_stem.split('_')[0].lower()
        tablename = f"public.parcels_{abbrv}"
        
        # Commands to remove existing data before appending newly refreshed ones
        remove_existing_sql = f"DELETE FROM {tablename} WHERE filename_stem = '{filename_stem}' ;"
        psql_connString = f"host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}"
        psql_cmd = f"psql '{psql_connString}' -c \"{remove_existing_sql}\""
        deleteCmds.append(psql_cmd)
            
        # https://gdal.org/programs/ogr2ogr.html & PostgreSQL output driver: https://gdal.org/drivers/vector/pg.html
        cmd = f"  ogr2ogr -f 'PostgreSQL' {connString} -skipfailures -unsetFid -progress -addfields -nlt 'PROMOTE_TO_MULTI' " 
        # Need the $ to escape the single quote (') inside ', for bash to process the -sql select command: https://stackoverflow.com/a/8254156
        cmd += f" -sql $'SELECT \\'{filename_stem}\\' AS filename_stem, {sql_select_fields}  FROM \"{filename_stem}\" ' "
        input_json =  f'{REGRID_BUCKET_DIR}{filename_stem}.json'
        cmd += f" '{input_json}' -nln '{tablename}' --config PG_USE_COPY YES ; "

        uploadCmds.append(cmd)
    
    with open(postgis_update_cmds_file, 'w') as f:
        f.writelines('\n'.join(uploadCmds))
        print(f"Ogr2ogr-Postgis UPLOAD/UPDATE commands are ready to be executed at {postgis_update_cmds_file}")
    with open(postgis_delete_cmds_file, 'w') as f:
        f.writelines('\n'.join(deleteCmds))
        print(f"Ogr2ogr-Postgis DELETE commands are ready to be executed at {postgis_delete_cmds_file}")
    # store logs   
    with open(f"./logs/{latest_refresh_date}_{postgis_update_cmds_file}" , 'w') as f:
        f.writelines('\n'.join(uploadCmds))
    with open(f"./logs/{latest_refresh_date}_{postgis_delete_cmds_file}" , 'w') as f:
        f.writelines('\n'.join(uploadCmds))

        
# generate mbtiles tiling cmds
def gen_tippecanoe_commands(updated_states, latest_refresh_date):
    print(f"Starting gen_tippecanoe_commands")
    updateCmds = []
    for abbrv in updated_states:
        mbtile_name = f"parcels_{abbrv}"
        
        # assemble the command
        tippecanoeCmd = "/usr/local/bin/tippecanoe -zg -Z10 --extend-zooms-if-still-dropping --drop-densest-as-needed "
        tippecanoeCmd += f"--force -o  ./data/{mbtile_name}.mbtiles -l {mbtile_name} "
        tippecanoeCmd += "-y ll_uuid -y county -y ll_bldg_count -y lbcs_function -y lbcs_activity -y lbcs_structure -y usedesc -y rdi -y dpv_type -y owner -y address -y address2 -y scity -y szip "
        tippecanoeCmd = tippecanoeCmd.split()
        # ensure only tiling counties included in verse.csv filename_stem
        state_all_jsons_filename_stem = set(name for name in all_bucket_filename_stem if name.split('_')[0] == abbrv).intersection(all_valid_filename_stem)
        state_all_jsons = sorted(f"{REGRID_BUCKET_DIR}{name}.json" for name in state_all_jsons_filename_stem)
        tippecanoeCmd += state_all_jsons
        updateCmds.append(" ".join(tippecanoeCmd))
        
    with open(mbtiles_update_cmds_file, 'w') as f:
        f.writelines('\n'.join(updateCmds))
        print(f"mbtiles commands are ready to be executed at {mbtiles_update_cmds_file}")
    with open(f"./logs/{latest_refresh_date}_{mbtiles_update_cmds_file}" , 'w') as f:
        f.writelines('\n'.join(updateCmds))

        
# TODO LATER: ADD NEW COLUMN TO EXISTING DATA
# maybe to use: sqlalchemy: pg INSERT WHERE ...key on `ll_uuid` or maybe combo(geoid, ogc_fid) [?]
# OR just re-import all tables!

### from_postgis: might be helpful later 
# - to_postgis: failed, and not needed



# MAIN MAIN FUNCTION CALL
main() 