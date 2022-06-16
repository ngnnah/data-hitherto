import sys
import pandas as pd
import json

attom_fromraw_cols = [
    '[ATTOM ID]', 'PropertyAddressFull',
    'AreaBuilding', 'AreaBuildingDefinitionCode', 'AreaLotSF', 'BuildingsCount', 'CompanyFlag', 
    'ContactOwnerMailAddressFull', 'ContactOwnerMailAddressInfoFormat', 'GeoQuality', 
    'LegalDescription', 'OwnerTypeDescription1', 'ParcelNumberRaw', 'PartyOwner1NameFull', 'PartyOwner2NameFull', 
    'PropertyAddressCity', 
    'PropertyAddressZIP', 
    'PropertyLatitude', 'PropertyLongitude', 
    'PropertyUseGroup', 'PropertyUseStandardized', 'RoomsCount', 'StatusOwnerOccupiedFlag', 'StoriesCount', 'UnitsCount', 
    'TaxMarketValueYear', 'TaxMarketValueImprovements', 'YearBuilt'
]


# 50 states + DC + 6 territories (PR, VI, AS, GU, MP, UM)
SF57 = {'01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR', '74': 'UM', '78': 'VI'}
SF57R = {value : key for (key, value) in SF57.items()}

# PARAM
input_file = sys.argv[1].lower()
zip_order = input_file.split('/')[-1][:2] # e.g. 03, or 11


# NOTE: regarding encoding guesses: 
# enca command shows: 7bit ASCII characters ; CRLF line terminators
# file command shows: ASCII text, with very long lines, with CRLF line terminators
# NOT RELEVANT: `In text mode, if encoding is not specified the encoding used is platform-dependent: locale.getpreferredencoding(False) is called to get the current locale encoding. `
# even though nowadays everything is utf-8 >>> ERROR: 'utf-8' codec can't decode byte 0x9d in position 5611: invalid start byte
# check for encoding [guess] with the file shell command in linux, e.g. file -i data/*.txt
# file command guessed wrong: encoding = us-ascii >>> ERROR: 'ascii' codec can't decode byte 0x9d in position 5611: ordinal not in range(128)
# ATTOM DATA response:  Our files are ANSI-1252 encoding
# encoding: EASY FIX via an errors handler 


possible_bad_values = {"", " ", "null", "Null", "None", "NONE", "none", "nan", "Nan"}
# MAKE SURE ALL 5 address1 FIELDS ARE IN CORRECT ORDERS
# e.g. addressFull = 145 15TH ST NE APT 843 = 145	NaN	15TH	ST	NE, and address2=	APT	843 
address1_fields = [
    'PropertyAddressHouseNumber', 'PropertyAddressStreetDirection', 'PropertyAddressStreetName', 'PropertyAddressStreetSuffix', 'PropertyAddressStreetPostDirection',]

# PARSE THE HEADER 
with open(input_file, errors='ignore') as f:
    for _, line in zip(range(1), f):
        line = line.strip().split('\t')
        # find indices of needed attom attributes
        header = {col_name : idx for idx, col_name in enumerate(line)} 

    # IMPORTANT: make sure keep_col_indices has same col orders as in attom_fromraw_cols
    keep_col_indices = [header[col_name] for col_name in attom_fromraw_cols]
    print("START OF ", input_file, zip_order, keep_col_indices)
    
    
    # PARSE THE BODY (line0 is the header)
    with open(input_file, errors='replace') as f:
        line_position = 0
        next(f)
        for line in f:
            line_position += 1
            line = line.strip().split('\t')
            sf_code = line[header['SitusStateCode']]
            
            # Rows without SitusStateCode are not yet processed
            if not sf_code:
                print(f"ALERT: null statecode file@row= {input_file}@{line_position}")
            elif sf_code not in SF57R:
                    print(f"ALERT: statecode outside SF57R, {sf_code}, file@row= {input_file}@{line_position}")
            else: # WRITE to individual state df
                
                # TODO: remove test
                if sf_code != "MO": continue
                
                # assemble address1 component
                # line[header[col_name]] is the cell value
                address1 = ["" if line[header[col_name]] in possible_bad_values 
                        else line[header[col_name]] for col_name in address1_fields]
                # Dont forget the space in join! 
                address1 = " ".join([v.strip() for v in address1 if v.strip()])
                # add strip() condition in case address1 = " ".join[] = ""
                address1 = address1 if address1.strip() else None

                # assemble address2 component
                # address2_fields = ['PropertyAddressUnitPrefix', 'PropertyAddressUnitValue']
                unit_prefix = line[header['PropertyAddressUnitPrefix']]
                unit_value = line[header['PropertyAddressUnitValue']] 
                UNIT_DELI = "@@"
                if unit_prefix in possible_bad_values:
                    if unit_value in possible_bad_values: 
                        address2 = None
                    else: 
                        address2 = UNIT_DELI + unit_value
                else:
                    if unit_value in possible_bad_values: 
                        address2 = unit_prefix + UNIT_DELI
                    else: 
                        address2 = unit_prefix + UNIT_DELI + unit_value

                # write to county files, e.g. AK_DENALI.json
                county = line[header['SitusCounty']]
                county = "".join(county.split())
                stem = (sf_code + '_' + county).lower()                    
                file = f'data/attom/{stem}.json'

                # select columns; and add addresses, and source locations (zip file, line positions)
                line = [None if value in possible_bad_values else value for value in 
                         [line[idx] for idx in keep_col_indices] + [address1, address2, zip_order, line_position]]
                
                with open(file, 'a') as f:
                    # NOTE: cannot use delimiters: pipe (|) or comma (,) , or any other delimeters 
                    # as the values contain these symbols, the encode/decode will be wrong; i.e. no csv/txt outputs!
                    f.write(json.dumps(line) + '\n')                    




