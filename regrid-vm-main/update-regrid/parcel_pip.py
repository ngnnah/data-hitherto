import sys
import geopandas as gp
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from glob import glob
import numpy as np

# ES dev instance
ES_DEV = Elasticsearch(['https://3d6a9dd50c7c49c9ab5d23b6891bc03e.us-central1.gcp.cloud.es.io:9243'], 
                    http_auth=('elastic', 'WMzYk5RXyzE7MRShwPVwHzPX'), timeout=30)

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
SF52R = {value : key for (key, value) in SF52.items()}


# regridcols = ['pid', 'addr', 'cbg', 'lat', 'lon',  'activity', 'function', 'structure',   
# 'dpv', 'dpv_type', 'rdi', 'building_count', 'footprint']


def upload_ES_df(input_df, index_name, idCol = 'GEOID', es_instance = ES_DEV):
    # actiondf: details all actions, contains 5 cols: _id, doc, _index, _op_type, doc_as_upsert
    # use idCol as document _id
    actiondf = input_df[[idCol]].copy().rename(columns={idCol:'_id'})
    # a dict of all the data to be updated; does contain idCol which will be uploaded like a regular column
    actiondf['doc'] = input_df.to_dict('records')  
    del input_df
    # Some constants
    actiondf['_index'] = index_name
    actiondf['_op_type'] = 'update'
    actiondf['doc_as_upsert'] = True
    
    print(f"Start uploading {len(actiondf)} records to {index_name}")
    helpers.bulk(es_instance, actiondf.to_dict('records'))
    print(f"Completed uploading {len(actiondf)} records to {index_name}")

    
def cat_parcel_conditions(activity, function, structure):
    if '1' in (activity, function, structure):
        return 'resi'
    if '2' in (activity, function, structure):
        return 'commer'
    if ('4' in (activity, function, structure)) or ('6' in (activity, function, structure)):
        return 'infra'
    if '8' in (activity, function, structure) or function == '9': 
        return 'agri'
    if function in '357' or activity in '357': # Higher priority
        return 'commer'
    if structure in '357': # Lower priority
        return 'infra'
    return 'rem' # ALL OTHER
cat_parcel_vectorize = np.vectorize(cat_parcel_conditions)



def main(state_abbrv):
    sf = SF52R[STATE_ABBRV]
    print(f"{sf} {STATE_ABBRV} Start processing")

    # # NOTE: It's wrong to only process newly refreshed counties; 
    ## BECAUSE: cannot locate what census block to drop before uploading (i.e. dropping cb that contain no points is wrong) 
    # # INSTEAD: get parcel count for all census blocks, whenever there's an update for a state
    county_lines = glob(f"./processed-jsons/{state_abbrv}_*.json")
    # assemble state dataframe from individual county jsonl
    state_df = []
    for jsonl in county_lines:
        df = pd.read_json(jsonl)
        if not df.empty:
            state_df.append(df)
    state_df = pd.concat(state_df)
    

    if state_df.pid.isnull().sum() != 0:
        print(f"ALERT: {sf} {STATE_ABBRV} STOPPING PREMATURELY! There are #{state_df.pid.isnull().sum()} null pid: SHOULD BE ZERO") 
    else:
        print(f"{sf} {STATE_ABBRV} Number of parcels: {state_df.shape}")
        # READING TIGER SHP
        tiger_url = f"https://www2.census.gov/geo/tiger/TIGER2019/TABBLOCK/tl_2019_{sf}_tabblock10.zip" 
        state_cb = gp.read_file(tiger_url)
        state_cb = state_cb[['GEOID10', 'geometry']]
        state_cb = state_cb.rename({"GEOID10": "GEOID"}, axis = 1)
        
        print(f"{sf} {SF52[sf]} Completed reading TIGER shp: {state_cb.shape=}")
        # NY: tiger download takes 25s, about 15 parcels per census block

        # NY= 2min: long time
        state_df =  gp.GeoDataFrame(state_df, geometry=gp.points_from_xy(state_df.lon, state_df.lat, crs=state_cb.crs)) 

        
        # # IF want to ssee diff_cbg (from regrid censusblockgroup col v.s. result of spatial-join using regrid lat/lon)
        # BLOCGROUP_LENGTH = 12
        # state_df['cbg'] = state_df['cbg'].astype(str).str.zfill(BLOCGROUP_LENGTH)
        # state_cb['cbg'] = state_cb['GEOID'].str[:BLOCGROUP_LENGTH]
       
        # SPATIAL JOIN = BIG TIME BOTTLENECK: NY= 4min: also long time
        state_df = gp.sjoin(state_df, state_cb, how='left', predicate='within')

        # # DISOWNED PARCELS: parcels with (lat, lon) not found within state boundaries i.e. parcels w/o GEOID!
        # disowned_parcels = state_df['GEOID'].isnull().sum()
        # print(f"Number of disowned parcels: {disowned_parcels=}, {(disowned_parcels/state_df.shape[0] * 100) :.5f}%")
        # # DIFF_CBG
        # diff_cbg = state_df.loc[ state_df['cbg_left'] != state_df['cbg_right']]
        # print(f"Percentage of diff_cbg: {(diff_cbg.shape[0] / len(state_df) * 100):.2f} %")
        # state_df.drop(columns=['cbg_left', 'cbg_right'], inplace=True)
        
        # state_cb: only need complete list of GEOID now (no longer need tiger geom)
        state_cb = state_cb[['GEOID']] 
        # drop now-redundant cols from state_df to reduce df size
        state_df.drop(columns=['geometry', 'index_right',], inplace=True)

        # Census-block groupby and agg: parcel classication
        state_df['actv'] = (state_df['activity'].fillna(0) // 1000).astype(int).astype(str)
        state_df['func'] = (state_df['function'].fillna(0) // 1000).astype(int).astype(str)
        state_df['strc'] = (state_df['structure'].fillna(0) // 1000).astype(int).astype(str)


        # https://support.regrid.com/articles/lbcs-keys/#function-classifications
        # func: combine a parcel's land use codes of (activity, function, structure)
        state_df['landuse'] = cat_parcel_vectorize(state_df['actv'], state_df['func'], state_df['strc'])
        state_df['lat'] = state_df['lat'].astype(str)
        state_df['lon'] = state_df['lon'].astype(str)


        # UPLOAD TO parcels_{sf}: select columns, fill null, coerce col types, and upload to parcels_{sf}
        parcels_index_cols = ['pid', 'addr', 'lat', 'lon', 'activity', 'function', 'structure', 
                              'dpv', 'dpv_type', 'rdi', 'building_count', 'footprint',  'GEOID', 'landuse'] 


        parcels_df = state_df[parcels_index_cols].copy()
        if parcels_df.pid.isnull().sum() != 0:
            print(f"ALERT: {sf} {STATE_ABBRV} there are #{parcels_df.pid.isnull().sum()} null pid: SHOULD BE ZERO")

        parcels_df['activity'] = (parcels_df['activity'].fillna(0)).astype(int).astype(str)
        parcels_df['function'] = (parcels_df['function'].fillna(0)).astype(int).astype(str)
        parcels_df['structure'] = (parcels_df['structure'].fillna(0)).astype(int).astype(str)

        for col in parcels_df.select_dtypes('number'):
            parcels_df[col].fillna(0, inplace=True)
        for col in parcels_df.select_dtypes('object'):
            parcels_df[col].fillna("", inplace=True)    

        if parcels_df.isnull().sum().sum() != 0:
            print(f"ALERT: {sf} {STATE_ABBRV} {parcels_df.shape}, #nulls={parcels_df.isnull().sum().sum()}. Make sure df contains no null values, before uploading to ES")
        else:
            # NOTE: ID on pid, instead of the regular GEOID (as in bossdata)
            parcels_df.to_csv(f'es/parcels_{sf}.csv', index=False)
            print(f"{sf} {STATE_ABBRV}: {parcels_df.shape} parcels ready to be uploaded to ES parcels_{sf}")
            # ACTUAL UPLOADING TO ES
            upload_ES_df(parcels_df, f'parcels_{sf}', idCol = 'pid')

        del parcels_df
        
        # GROUPBY for parcel land-use classication
        parcel_cat_df = state_df.groupby(['GEOID', 'landuse']).size().unstack(fill_value = 0)
        # in case: e.g. DC doesn't have any agri parcels, so this df will not have the agri column at all
        for col in ['resi', 'agri', 'commer', 'infra', 'rem']:
            if col not in parcel_cat_df:
                parcel_cat_df[col] = 0
        parcel_cat_df['total'] = parcel_cat_df.sum(axis=1)
        parcel_cat_df['valid'] = parcel_cat_df['total'] - parcel_cat_df['rem']
        parcel_cat_df = parcel_cat_df.rename(columns={'resi': 'parcelNumResi', 'agri': 'parcelNumAgri', 
                                                      'commer': 'parcelNumCommer', 'infra': 'parcelNumInfra', 
                                                      'rem': 'parcelNumRem', 'valid': 'parcelNumValid', 'total': 'parcelNumTotal'})

        # SIMPLY REUSING pacrelAddresses field (defined in previous bossdata* mappings), but to be accurate it should be: parcelIds or parcel_lluid or contained_parcels_ll_uuid
        parcel_cat_df['parcelAddresses'] = state_df.groupby('GEOID').agg({'pid': ';'.join})

        parcel_cat_df[['parcelBuildingCount', 'parcelBuildingFootprint']] = state_df.groupby('GEOID')[['building_count', 'footprint']].sum()
        parcel_cat_df.reset_index(inplace=True)
        
        del state_df
        
        print(f"{sf} {STATE_ABBRV} Number of GEOIDs that contain parcels: {parcel_cat_df.shape}")
        if parcel_cat_df.isnull().sum().sum() != 0:
            print(f"{sf} {STATE_ABBRV} parcel_cat_df SHOULD CONTAIN NO NULL: {parcel_cat_df.isnull().sum().sum()=}")


        # PROCESS the df to UPLOAD TO ES!
        es_cols = ['parcelNumTotal', 'parcelNumValid', 'parcelNumRem',
                   'parcelNumResi', 'parcelNumAgri', 'parcelNumCommer', 'parcelNumInfra',  
                   # SIMPLY REUSING pacrelAddresses field (defined in previous bossdata* mappings), but to be accurate it should be: parcelIds or parcel_lluid or contained_parcels_ll_uuid
                   'parcelAddresses', 'parcelBuildingCount', 'parcelBuildingFootprint']
        es_df = state_cb.merge(parcel_cat_df, how='left', on="GEOID")[['GEOID'] + es_cols] 
        
        del state_cb
        del parcel_cat_df
        
        for col in es_cols:
            if col == 'parcelAddresses':
                es_df[col] = es_df[col].fillna("").astype(str)
            else:
                es_df[col] = es_df[col].fillna(0).astype(int)

        # parcel count: SUM check
        parcelNum_sum_check = es_df[['parcelNumTotal', 'parcelNumValid', 'parcelNumResi', 'parcelNumAgri',  'parcelNumCommer', 'parcelNumInfra', 'parcelNumRem']].sum()
        if (parcelNum_sum_check[0] - sum(parcelNum_sum_check[2:])) != 0:
            print(f"ALERT: {sf} {STATE_ABBRV} DOUBLE CHECK parcelNum fields: got wrong result for breaking down parcelNumTotal into smaller fields")

        if es_df.isnull().sum().sum() != 0:
            print(f"ALERT: {sf} {STATE_ABBRV} es_df, before uploading to ES, should contain no null")

        index_name = f"bossdata{sf}"
        es_df.to_csv(f'es/new_{index_name}.csv', index=False)
        print(f"{sf} {STATE_ABBRV} {es_df.shape} Saved to es/new_{index_name}.csv; and ready to upload to {index_name}")
        # ACTUAL UPLOADING TO ES
        upload_ES_df(es_df, index_name, idCol = 'GEOID')         
    
        del es_df
    
    
    
state_abbrv = sys.argv[1].lower()
STATE_ABBRV = state_abbrv.upper()
print(f"********main main main program for: {STATE_ABBRV}*******************")
main(state_abbrv)
