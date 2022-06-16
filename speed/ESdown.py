import sys
import pandas as pd
from elasticsearch import Elasticsearch, helpers

ES_DEV = Elasticsearch(['YOUR ES HOST'], 
                    http_auth=('ES LOGIN', 'ES PASS'), timeout=30)

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

abbrv = sys.argv[1].lower()
ABBRV = abbrv.upper()
sf = SF52R[ABBRV]
index_name = f"bossdata{sf}"


BASE_COLS = ['GEOID', ]

# Would be used for NTIA speed category
NTIA_SPEED_COLS = [
'numISPfiber',
'numISPother',
'numISPwireless',
'MaxConsumerDown98',
'MaxConsumerUp98',
]

# These values are used for MLAB prediction; which often stay rather constant, but they could be updated at moment notice
MLAB_PREDICTION_COLS = [
    'CMC', 'Education', 'Health', 
    'POP2019', 'Public Admin', 'age65overper', 'asianper', 'bachelorper', 
    'blackper', 'hh2020', 'hu2020', 'landareaSqmi', 'lengthMile', 
    'maxadownFiber', 'maxadownOther', 'maxadownWireless', 'maxadupFiber', 'maxadupOther', 'maxadupWireless',
    'mhincome', 'nativeper', 'nocomputerper_ct', 'nointernetper', 'nointernetper_ct', 
    'numISPcomm', 'numISPresi', 'num_household', 'num_household_ct', 'num_housingunit', 'otherraceper', 
    'parcelNumAgri', 'parcelNumCommer', 'parcelNumInfra', 'parcelNumResi', 
    'parcelNumRem', 'parcelNumValid', 'parcelNumTotal',
    'parcelBuildingCount', 'parcelBuildingFootprint',
    "cafiiLocation", 'pop2020', 'povertybelow15', 'povertybelow15_ct', 
    'povertybelow20_ct', 'povertyper', 'povertyper_ct', 
    'rdofLocation', 'rdofReserve', 'whiteper'] 

ES_DOWNLOAD_COLS = BASE_COLS + NTIA_SPEED_COLS + MLAB_PREDICTION_COLS
    
def download_ES(index_name, sf, es_instance = ES_DEV):
    print('Start downloading ES index ', index_name)
    match_query = {
        "query": {"match_all": {}},
        "_source": {
            "includes": ES_DOWNLOAD_COLS
        },
    }
    res_gen = helpers.scan(es_instance, query= match_query, index= index_name) # 3 x faster than (match_all)
    df = pd.DataFrame([record['_source'] for record in res_gen])
    print(f'Completed downloading ES index {index_name}, {df.shape=}')
    return df


df = download_ES(index_name, sf)
# Save NTIA df
path = f'Elasticsearch/ntia_{index_name}.csv'
df[BASE_COLS + NTIA_SPEED_COLS].to_csv(path, index=False)
print(f"Saved ntia_df {df.shape} to ", path)

# Save MLAB_prediction df
path = f'Elasticsearch/mlab_prediction_{index_name}.csv'
df[BASE_COLS + MLAB_PREDICTION_COLS].to_csv(path, index=False)
print(f"Saved mlab_prediction_df {df.shape} to ", path)

