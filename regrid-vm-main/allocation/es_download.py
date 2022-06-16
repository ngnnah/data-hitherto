import sys
from elasticsearch import Elasticsearch, helpers
import pandas as pd

es = Elasticsearch(['https://3d6a9dd50c7c49c9ab5d23b6891bc03e.us-central1.gcp.cloud.es.io:9243'], 
                    http_auth=('elastic', 'WMzYk5RXyzE7MRShwPVwHzPX'), timeout=100, max_retries=2, retry_on_timeout=True)

STATE_FIPS_DICT_52 = { '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', 
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
R_STATE_FIPS_DICT_52 = {value : key for (key, value) in STATE_FIPS_DICT_52.items()}

passing_cols = ['GEOID', 'CountyName', 
                'Health', "Public Admin", "Education", 
                "POP2019", "mhincome", 
                "num_household", "hh2020", "num_housingunit", "hu2020",
                "parcelNumAddr", "parcelNumResi", "parcelNumAgri", "parcelNumCommer", "parcelNumInfra",
                "nointernetper", "speedRankReadyRaw", "location"]

state_abbrv = sys.argv[1].lower()

# WORKAROUND FOR NOW: skip field nointernetper for OKLAHOMA; and coordinates geo_shape instead of location
# "Transportation" exists in bossdata* mapping, but not populated
if state_abbrv == 'ok':
    passing_cols = ['GEOID', 'CountyName', 
                    'Health', "Public Admin", "Education", 
                    "POP2019", "mhincome", 
                    "num_household", "hh2020", "num_housingunit", "hu2020", 
                    "parcelNumAddr", "parcelNumResi", "parcelNumAgri", "parcelNumCommer", "parcelNumInfra",
                    "speedRankReadyRaw", "coordinates"]

sf = R_STATE_FIPS_DICT_52[state_abbrv.upper()]

index_name = f"bossdata{sf}"
print('Querying/Downloading index ', index_name, state_abbrv)
match_all_query = {"query": {"match_all": {}}}
res_gen = helpers.scan(es, query= match_all_query, index= index_name)
df = pd.DataFrame([record['_source'] for record in res_gen])
df = df[passing_cols]
df = df.rename(columns = {"coordinates": "location"})
df.to_csv(f"es2020/es_{state_abbrv.upper()}.csv", index=False)
    
    
    