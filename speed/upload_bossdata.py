import sys
import geopandas as gp
import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch, helpers
ES_DEV = Elasticsearch(['YOUR ES HOST'], http_auth=('ES LOGIN', 'ES PASS'), timeout=30)

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


# Main arguments
abbrv = sys.argv[1].lower()
ABBRV = abbrv.upper()
sf = SF52R[ABBRV]

# 13 cols
OOKLA_END_COLS = [
'maxDownloadMbpsOokla', 'maxUploadMbpsOokla',
'meanDownloadMbpsOokla', 'meanUploadMbpsOokla',
'medDownloadMbpsOokla', 'medUploadMbpsOokla',
'minDownloadMbpsOokla', 'minUploadMbpsOokla',
'latencyOokla',
'numDeviceOokla', 'numTestOokla',
'speedCatOokla', 'speedSourceOokla',
]
# 16 cols
MLAB_END_COLS = [
'maxDownloadMbpsMlab', 'maxUploadMbpsMlab',
'minDownloadMbpsMlab', 'minUploadMbpsMlab',
'numDeviceDownloadMlab', 'numDeviceUploadMlab',
'numTestDownloadMlab', 'numTestUploadMlab',
'meanDownloadMbpsMlab', 'meanUploadMbpsMlab',
'medDownloadMbpsMlab', 'medUploadMbpsMlab',
'latencyMlab', 'lossrateMlab',
'speedCatMlab','speedSourceMlab',
]
    
def upload_ES_df(input_df, index_name, idCol = 'GEOID', es_instance = ES_DEV):
    actiondf = input_df[[idCol]].copy().rename(columns={idCol:'_id'})
    actiondf['doc'] = input_df.to_dict('records')  
    del input_df
    actiondf['_index'] = index_name
    actiondf['_op_type'] = 'update'
    actiondf['doc_as_upsert'] = True
    
    print(f"Start uploading {len(actiondf)} records to {index_name}")
    helpers.bulk(es_instance, actiondf.to_dict('records'))
    print(f"Completed uploading {len(actiondf)} records to {index_name}")
    


# MAIN MAIN MAIN

QUARTER = '2021Q4' # IMPORTANT: Use latest quarter!
file_path = f'speed_ready_upload/{QUARTER}_{sf}.csv'
# SPEED_COLS = 4 + 13 + 16 = 33 cols
SPEED_COLS = set(['GEOID', 'speedCatNtia', 'speedSourceNtia', 'speedRankReadyRaw'] 
                 + OOKLA_END_COLS + MLAB_END_COLS)

main_df = pd.read_csv(file_path)[SPEED_COLS]
# ensure that GEOID is of type str, with full CB_LENGTH
main_df.GEOID = main_df.GEOID.astype(str).str.zfill(CB_LENGTH)


s = main_df.isnull().sum()
if s.sum():
    print(f"ALERT: {QUARTER}_{sf} df contain nulls: {s[s>0]}")
else:
    # coerce cols to integer types
    int_cols = [
     'latencyMlab',
     'latencyOokla',
     'numDeviceDownloadMlab',
     'numDeviceOokla',
     'numDeviceUploadMlab',
     'numTestDownloadMlab',
     'numTestOokla',
     'numTestUploadMlab',
     'speedCatMlab',
     'speedCatNtia',
     'speedCatOokla',    
    ]
    main_df[int_cols] = main_df[int_cols].round(0).astype(int)
    # round float cols to 2 digit
    float_cols = [
     'lossrateMlab',
     'maxDownloadMbpsMlab',
     'maxDownloadMbpsOokla',
     'maxUploadMbpsMlab',
     'maxUploadMbpsOokla',
     'meanDownloadMbpsMlab',
     'meanDownloadMbpsOokla',
     'meanUploadMbpsMlab',
     'meanUploadMbpsOokla',
     'medDownloadMbpsMlab',
     'medDownloadMbpsOokla',
     'medUploadMbpsMlab',
     'medUploadMbpsOokla',
     'minDownloadMbpsMlab',
     'minDownloadMbpsOokla',
     'minUploadMbpsMlab',
     'minUploadMbpsOokla',    
    ]
    main_df[float_cols] = main_df[float_cols].round(2).astype(float)
    
    # UPLOAD to Elasticsearch
    index_name = f'bossdata{sf}'
    
    # TEST on nhat_test2 index FIRST
    # index_name = 'nhat_test2'
    
    print(f"{QUARTER}_{sf=}/{SF52[sf]}, {main_df.shape=} ready to be uploaded to {index_name}")
    upload_ES_df(main_df, index_name)