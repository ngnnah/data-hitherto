import sys
import json
from pathlib import Path
from glob import glob
import ijson.backends.yajl2_c as ijson
import datetime
today = datetime.datetime.now().strftime('%Y-%m-%d')


def stream_parse(input_file):
    with open(input_file, 'rb') as file:
        properties = ijson.items(file, 'features.item.properties')
        count=0
        geoid=""
        for prop in properties:
            count+=1
            if count: geoid = prop['geoid']
        return geoid, count

# SLOW:: 
def main_state(state_abbrv):
    print("==========================================")
    print(f"Processing {state_abbrv=}")
    total_parcels = 0 
    counties = glob(f'/home/nhat/regrid-bucket/{state_abbrv}_*.json')
    print(f"Number of counties in {state_abbrv=} : {len(counties)=}")
    for input_file in counties:
        name = Path(input_file).stem
        geoid, count = stream_parse(input_file)
        total_parcels += count
        row = {'state': state_abbrv, 'name': name, 'geoid': geoid, 'count': count}
        row = json.dumps(row) + '\n'
        with open(f'parcelCount-state_{today}.txt', 'a', newline='\n') as f:
            f.write(row)
    print(f"Completed counting parcels for {state_abbrv=}: {total_parcels=}")

    
    
# MUCH FASTER # dont have to wait on LARGE/slow states like ca, tx, and fl ; also can launch hundreds of jobs at once instead of just 52 state jobs
# AS EXPECTED: 2 last runners are:
# {"state": "tx", "name": "tx_harris", "geoid": "48201", "count": 1441441}
# {"state": "ca", "name": "ca_los-angeles", "geoid": "06037", "count": 2414534}
def main_county(input_file):
    name = Path(input_file).stem
    state_abbrv = name.split('_')[0]
    print(f"Parsing {name=}")
    geoid, count = stream_parse(input_file)
    row = {'state': state_abbrv, 'name': name, 'geoid': geoid, 'count': count}
    print(f"{row=}")
    row = json.dumps(row) + '\n'
    with open(f'parcelCount-county_{today}.txt', 'a') as f:
        f.write(row)
    return

# MAIN
arg, level = sys.argv[1], sys.argv[2]

if level == 'state':
    main_state(arg.lower()) 
elif level == 'county':
    main_county(arg)



