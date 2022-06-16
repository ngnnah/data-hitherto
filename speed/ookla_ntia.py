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

# Main arguments
abbrv = sys.argv[1].lower()
ABBRV = abbrv.upper()
sf = SF52R[ABBRV]

STATE_LENGTH, COUNTY_LENGTH, CT_LENGTH, CBG_LENGTH, CB_LENGTH = 2, 5, 11, 12, 15
# EPSG:4269 Geodetic coordinate system for North America - onshore and offshore
# EPSG:4326 - World Geodetic System 1984, used in GPS
CRS_TIGER, CRS_OOKLA = 4269, 4326



def cat_NTIA_speed_conditions(down, up, numISPfiber, numISPother, numISPwireless):
    if (down < 25 or up < 3 or (numISPfiber < 1 and numISPother < 1 and numISPwireless < 1)):
        # NTIA fields could be None/NaN/null
        return 0
    if down < 100 or up < 20: return 1
    return 2
cat_speed_NTIA_vectorize = np.vectorize(cat_NTIA_speed_conditions)

def cat_OOKLA_speed_conditions(down, up, latency):
    if down < 25 or up < 3:
        return 0
    if down < 100 or up < 20 or latency > 100: 
        return 1
    return 2
cat_speed_OOKLA_vectorize = np.vectorize(cat_OOKLA_speed_conditions)


# PART I: NTIA speed category
# Start with NTIA_bossdata* df
index_name = f"bossdata{sf}"
ntia_df = pd.read_csv(f'Elasticsearch/ntia_{index_name}.csv')
ntia_df.GEOID = ntia_df.GEOID.astype(str).str.zfill(CB_LENGTH)
ntia_df['GEOID_cbg'] = ntia_df.GEOID.str[:CBG_LENGTH]

ntia_df['speedSourceNtia'] = np.where(ntia_df[f'MaxConsumerDown98'].isnull() 
                                   | ntia_df[f'MaxConsumerUp98'].isnull(),
                              'notAvailableMaxConsumer98', 'maxConsumer98')

print("**********", ABBRV, sf, ntia_df.shape) 

# NaN values in ntia_df i.e. NTIA columns 
# (columns = ['MaxConsumerDown98', 'MaxConsumerUp98', 'numISPfiber', 'numISPother', 'numISPwireless']) 
# are most likely due to NOT PRESENT AT ALL i.e. most likely = 0
ntia_df.fillna(0, inplace=True)

# # Null check No.1 
if  ntia_df.isnull().sum().sum() != 0:
    null_sums = ntia_df.isnull().sum()
    print(f"ALERT! {ABBRV} {sf}: near the beginning, df CONTAINS {null_sums.sum()} NULL values: {null_sums[null_sums > 0]}")

# # speedCatNtia vectorized
ntia_df['speedCatNtia']= cat_speed_NTIA_vectorize(
    ntia_df['MaxConsumerDown98'], ntia_df['MaxConsumerUp98'],  
    ntia_df['numISPfiber'], ntia_df['numISPother'], ntia_df['numISPwireless'])


# # PART II: Ookla speed category
for QUARTER in ['2021Q1', '2021Q2', '2021Q3', '2021Q4']:
    ookla_tiles_dir = 'ookla_state_tiles/'
    # block GEOID, and GEOID_cbg
    ookla_censusblock = pd.read_csv(f'{ookla_tiles_dir}{QUARTER}_CB_{sf}.csv')
    ookla_censusblock.GEOID = ookla_censusblock.GEOID.astype(str).str.zfill(CB_LENGTH)
    ookla_censusblock['GEOID_cbg'] = ookla_censusblock.GEOID.str[:CBG_LENGTH]    
    # blockgroup GEOID_cbg
    ookla_blockgroup = pd.read_csv(f'{ookla_tiles_dir}{QUARTER}_CBG_{sf}.csv')
    ookla_blockgroup.GEOID_cbg = ookla_blockgroup.GEOID_cbg.astype(str).str.zfill(CBG_LENGTH)
    # TIGER CENSUS: CBG <> CB alignment check
    diff_cbg_cb = set(ookla_blockgroup.GEOID_cbg) - set(ookla_censusblock.GEOID.str[:CBG_LENGTH])
    diff_cb_cbg = set(ookla_censusblock.GEOID.str[:CBG_LENGTH]) - set(ookla_blockgroup.GEOID_cbg)
    if len(diff_cbg_cb) or len(diff_cb_cbg):
        print(f"*** ALERT *** {ABBRV} {sf} ookla_tiger GEOID-truncated != ookla_tiger GEOID_cbg: {diff_cb_cbg=} v.s. {diff_cbg_cb=}")

    # Interpolate CBG to CB    
    main_df = ntia_df.set_index('GEOID_cbg').join(ookla_blockgroup.set_index('GEOID_cbg'))
    main_df = main_df.set_index('GEOID')

    # update existing data using available ookla CB rows; 
    # df1.update(df2): pandas note: df1 should contain all columns in df2
    main_df.update(ookla_censusblock.set_index('GEOID'))


    # medianImputer missing ookla values at CB level
    # When one of (medDown, medUp, and latency) is missing, need to impute before can get speedCatOokla. Thus, update speedSourceOokla accordingly
    main_df.loc[main_df.medDownloadMbpsOokla.isnull() | 
                    main_df.medUploadMbpsOokla.isnull() | main_df.latencyOokla.isnull(), 
                ['speedSourceOokla']] = 'medianImputedAtCB'

    for col in ['meanDownloadMbpsOokla', 'meanUploadMbpsOokla', 
                'medDownloadMbpsOokla', 'medUploadMbpsOokla', 
                'maxDownloadMbpsOokla', 'maxUploadMbpsOokla', 
                'minDownloadMbpsOokla', 'minUploadMbpsOokla', 
                'numTestOokla', 'numDeviceOokla', 'latencyOokla']:
        main_df[col].fillna(main_df[col].median(), inplace=True)


    # speedCatOokla vectorized
    main_df['speedCatOokla']= cat_speed_OOKLA_vectorize(
        main_df['medDownloadMbpsOokla'], main_df['medUploadMbpsOokla'],  main_df['latencyOokla'])

    # # Null check No.2
    if  main_df.isnull().sum().sum() != 0:
        null_sums = main_df.isnull().sum()
        print(f"*** ALERT *** {ABBRV} {sf}: near the end, df CONTAINS {null_sums.sum()} NULL values: {null_sums[null_sums > 0]}")

    # FINALLY, save main_df for training MLAB later 
    file_path = f"ookla_ntia/{QUARTER}_CB_{sf}.csv"
    main_df = main_df.reset_index()
    main_df.to_csv(file_path, index=False)
    print(f"Saved {main_df.shape} to {file_path}")
