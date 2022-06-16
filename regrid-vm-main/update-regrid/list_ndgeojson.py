from glob import glob

SF57 = {'01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR', '74': 'UM', '78': 'VI'}
SF57R = {value : key for (key, value) in SF57.items()}

# Create file for parallel tippecanoe tiling commands
bucket = '/home/nhat/regrid-bucket/'
d = {}
tippecanoeCmds = []
for sf in SF57R:
    sf = sf.lower()
    stems = glob(f"{bucket}{sf}*.ndgeojson.gz")
    d[sf] = stems
    
    if len(stems) == 0:
        print("NO PARCEL IN statefips: ", sf)
    else:
        # # just the stem, no path
        # stems = [stem.split('/')[-1].split('.')[0][3:] for stem in stems]
        mbtile_name = f"parcels_{sf}"
        # assemble the command
        # If your input is formatted as newline-delimited GeoJSON, use -P to make input parsing a lot faster.
        tippecanoeCmd = "/usr/local/bin/tippecanoe -zg -Z10 -P --extend-zooms-if-still-dropping --drop-densest-as-needed "
        tippecanoeCmd += f"--force -o  /home/nhat/update-regrid/data/{mbtile_name}.mbtiles -l {mbtile_name} "
        tippecanoeCmd += "-y ll_uuid -y county -y ll_bldg_count -y lbcs_function -y lbcs_activity -y lbcs_structure -y usedesc -y rdi -y dpv_type -y owner -y address -y address2 -y scity -y szip "
        tippecanoeCmd = tippecanoeCmd.split()
        tippecanoeCmd += stems
        tippecanoeCmds.append(" ".join(tippecanoeCmd))
        
    with open('monthly_tippecanoe.txt', 'w') as f:
        f.writelines('\n'.join(tippecanoeCmds))
