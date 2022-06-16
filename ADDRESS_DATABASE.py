sf_code = 'AK'
date="jun15" 
print(f"START WORK ON {sf_code}")

import pandas as pd
import numpy as np
import json
import geopandas as gp
import h3
import h3pandas
from placekey.api import PlacekeyAPI
from keplergl import KeplerGl

from glob import glob
from multiprocess import Pool
from unsync import unsync
import gzip 
from zipfile import ZipFile as ZF
from shapely.geometry import shape
from collections import Counter


# 50 states + DC + 6 territories (PR, VI, AS, GU, MP, UM)
SF57 = {'01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR', '74': 'UM', '78': 'VI'}
SF57R = {value : key for (key, value) in SF57.items()}

def gen_h3_hex_vectors(lat, lon, res):
    return h3.geo_to_h3(lat, lon, res)
gen_h3_hex = np.vectorize(gen_h3_hex_vectors)


attom_sel_cols = ['[ATTOM ID]',     
    'LegalDescription', 'OwnerTypeDescription1', 'ParcelNumberRaw', 
    'PartyOwner1NameFull', 'PartyOwner2NameFull', 
    'PropertyAddressCity', 
    'PropertyAddressFull', 
    'PropertyAddressHouseNumber', 'PropertyAddressStreetDirection', 'PropertyAddressStreetName', 
    'PropertyAddressStreetPostDirection', 'PropertyAddressStreetSuffix', 'PropertyAddressUnitPrefix', 'PropertyAddressUnitValue', 
    'PropertyAddressZIP', 
    'PropertyLatitude', 'PropertyLongitude',]

# NOTE: geom = WTK string/object type; geometry = geopandas geometry type
CRS_REGRID_PARCELS = "EPSG:4326"

regrid_sel_cols = ['ll_uuid', 
    'address', 
   'address2', # i.e. alternative address for same parcel
   # 'original_address', # confirmed: all useless when (~address & original_address)
               'legaldesc', 'parcelnumb', 
    'saddno', 'saddpref', 'saddstr', 'saddsttyp', 'saddstsuf', 'sunit' ,
    'scity', 'szip', 'lat', 'lon', 
    'owner', 'owner2', ]

def owner_compare(df_compare, left, right):
    # only keep alphanumeric values ; note the additions of symbols
    keepchars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" + ",& "
    del_chars = ''.join(c for c in map(chr, range(1114111)) if not c in set(keepchars))
    # convert uppercases to lowercases
    keepchars_table = str.maketrans(keepchars, keepchars.lower(), del_chars)

    df_compare['owners'] = df_compare[left].str.translate(keepchars_table).str.split(',|&| ')
    df_compare['Owners'] = df_compare[right].str.translate(keepchars_table).str.split(',|&| ')

    def owners_combine(owners, Owners):
        if owners is not np.nan and Owners is not np.nan:
            seta = {own for own in owners if len(own) > 1}
            setb = {own for own in Owners if len(own) > 1}
            return True if seta.intersection(setb) else False
        return True
    owners_combine_vectorize = np.vectorize(owners_combine)
    return owners_combine_vectorize(df_compare['owners'], df_compare['Owners'])

attom_pk_maps = {
    'aid' : 'query_id', # unique row id
    'Lat' : 'latitude', 
    'Lon' : 'longitude', 
    'Address1' : 'street_address', # NOTE: using address1
    'ACity' : 'city',
    'AZip' : 'postal_code',
}

regrid_pk_maps = {
    'rid' : 'query_id', # unique row id
    'lat' : 'latitude', 
    'lon' : 'longitude', 
    'address1' : 'street_address', # NOTE: using address1
    'scity' : 'city',
    'szip' : 'postal_code', # e.g. 48103, 48104-3423
}

pk_api1 = PlacekeyAPI("KEY 1" )
pk_api2 = PlacekeyAPI("KEY 2" )

#  https://github.com/alex-sherman/unsync#multi-threading-an-io-bound-function
# Iterative API calls= slow! Use unsync that simplifies ThreadPoolExecutor's async tasks
# @unsync convert a regular synchronous func into a threaded Unfuture
def pk_call(df, sf_code, maps, orig_id, pk_api):
    def gen_placekey_df(df, maps):
        keep_cols = list(maps.values())
        # only need placekey cols
        # drop columns with names similar to Placekey required columns
        df = df.drop(columns=keep_cols, 
                     errors='ignore').rename(columns=maps)[keep_cols]

        # zipcode = 00000 means null; ATTOM zipcode could be a float, or None, ""
        # so, need a placeholder value to convert to int, zfill, back to np.nan
        NAN_ZIPS = "00000"
        df['postal_code'] = df['postal_code'].fillna(NAN_ZIPS).astype(int).astype(str).str.zfill(5).replace(NAN_ZIPS, np.nan)
        # print("Generated pk df ", df.shape)

        # OPTIONAL CLEANING
        possible_bad_values = ["", " ", "null", "Null", "None", "nan", "Nan"]  
        for bad_value in possible_bad_values:
            df = df.replace(bad_value, np.nan)
        # replace NoneType with np.nan    
        df.fillna(np.nan, inplace=True)
        return df   

    # Synchronous functions can be made to run asynchronously by executing them in a concurrent.ThreadPoolExecutor. 
    # This can be easily accomplished by marking (decorating) the regular function @unsync.
    @unsync
    # Placekey API lookup function
    def pk_lookup(df, pk_api):
        # add missing hard-coded columns (str type)
        df['iso_country_code'] = 'US' 
        # sf_code has GLOBAL SCOPE
        df['region'] = sf_code.upper()    
        df = json.loads(df.to_json(orient='records'))
        # Rate limit: 100 bulk req per min x 100 addrs per bulk req
        # i.e. 10,000 addr per min 
        responses =  pk_api.lookup_placekeys(df, 
                                        strict_address_match=False,
                                        strict_name_match=False, 
                                        # verbose=True,
                                       )
        # Clean the responses
        # print("number of requests sent: ", len(df))
        # print("total queries returned:", len(responses))
        # filter out invalid responses
        responses_cleaned = [resp for resp in responses if 'query_id' in resp]
        # print("total successful responses:", len(responses_cleaned))
        # print(f"COMPLETED querying placekey api. Total queries returned: {sf_code}, {len(responses)}")
        return pd.read_json(json.dumps(responses_cleaned), dtype={'query_id':str})


    # TODO opt: groupby address1+hex7: avoid duplicate Placekey API request!
    # assumption: there should be no duplicate address1 within same ahex7 (within 5km2)
    # will not filter: same addr & adjacent ahex7 - placekey API will verify!
    # FAILED: concat address with zip, eliminate repeated address (https://www.quora.com/Do-any-two-locations-with-the-same-street-address-also-have-the-same-ZIP-code)
    # also, groupby fails when zip is null!
    df_pk = gen_placekey_df(df, maps)

    # API REQUEST
    pk_res_unfuture =  pk_lookup(df_pk.copy(), pk_api) # # (148_487, 6) around 15min
    pk_res = pk_res_unfuture.result() # blocking 
    # Show API responses errors
    # if "error" in set(pk_res):
    #     print(orig_id, "# responses: ", pk_res.shape, "; responses errors: ")
    #     print(pk_res.error.value_counts())

    # split into 2 components -- only interest in results with pkwhat
    # this also filters rows with error i.e. no placekey
    if "placekey" in set(pk_res):
        pk_res[['pkwhat', 'pkwhere']] = pk_res.placekey.str.split(
            "@", expand=True).replace("", np.nan)
    else:
        pk_res[['placekey', 'pkwhat', 'pkwhere']] = np.nan

    # Save API responses!!
    pk_res.reset_index(drop=True).to_feather(f'placekeyed/{orig_id}_{sf_code}.ftr')

    # keep only results whith pkwhat components
    pk_res = pk_res[pk_res.pkwhat.notna()]
    # Merge with original df; keep only rows with placekey results
    df = pd.merge(df, pk_res, left_on = orig_id, 
      right_on="query_id", how='inner').drop(columns= ['query_id', 'error'], errors='ignore')
    return df


def regrid_landuse_classifier(acti, func, struc, site):
    # REGRID LAND USE CLASSIFICATION
    if 1 in (acti, func, struc):
        return 'resi'
    if 2 in (acti, func, struc) or func in set('357') or acti in set('357'):
        return 'biz'
    if 8 in (acti, struc) or func == 9: 
        return 'farm'
    if 4 in (acti, func) or 6 in (acti, func) or struc in set('34567'):
        return 'CAI'
    if site != 6: # no developed site on land
        return 'vacland'    
    return 'rem' # ALL OTHER LANDUSE GROUPS   


def rad_fields_vec(rid, aid, address1, addressSub, Address1, AddressSub, lat, lon, Lat, Lon, owner, owner2, Owner, Owner2):
    pId = rId = pAddress1 = pAddressSub = pOwner = pLat = pLon = np.nan

    rlist = [rid, address1, addressSub, lat, lon, owner, owner2]
    for r, addr1, addrSub, la, lo, own, own2 in zip(*rlist):
        pId = rId = r
        if addr1 is not None and addr1 == addr1: # not None, not na
            pId = rId = r
            pAddress1, pAddressSub = addr1, addrSub
        if la is not None and lo is not None and la == la and lo == lo:
            pLat, pLon = la, lo
        if own is not None and own == own:
            pOwner = own
        elif own2 is not None and own2 == own2:
            pOwner = own2
        if np.nan not in (pId, rId, pAddress1, pAddressSub, pOwner, pLat, 
           pLon):
            return "\t".join([str(k) for k in [pId, rId, pAddress1, pAddressSub, pOwner, pLat, pLon]])

    alist = [aid, Address1, AddressSub, Lat, Lon, Owner, Owner2]
    for a, addr1, addrSub, la, lo, own, own2 in zip(*alist):
        if pId != pId: # if have seen no regrid parcel 
            pId = a
        if pAddress1 != pAddress1 and addr1 is not None and addr1 == addr1: # not na
            pId = a # rId would still be Nan, or equal to a previous rid
            pAddress1, pAddressSub = addr1, addrSub
        if pLat != pLat and pLon != pLon and la is not None and lo is not None and la == la and lo == lo:
            pLat, pLon = la, lo
        if pOwner != pOwner and own is not None and own == own:
            pOwner = own
        elif pOwner != pOwner and own2 is not None and own2 == own2:
            pOwner = own2
        if np.nan not in (pId, rId, pAddress1, pAddressSub, pOwner, pLat, 
           pLon):
            return "\t".join([str(k) for k in [pId, rId, pAddress1, pAddressSub, pOwner, pLat, pLon]])
    return "\t".join([str(k) for k in [pId, rId, pAddress1, pAddressSub, pOwner, pLat, pLon]])

def rad_landuse_classifier_single(alanduse, rlanduse):
    # Higher to lower prioritization
    if alanduse in ('CAI', 'biz', 'resi'): # also in order of priority
        return alanduse
    if rlanduse in ('CAI', 'biz', 'resi'): # also in order of priority
        return rlanduse
    for weak in ('farm', 'vacland', 'rem'): # also in order of priority
        if weak in set((alanduse, rlanduse)):
            return weak

SORT_ORDER = {'CAI': 10, 'biz': 9, 'resi': 8, 'farm': 7, 'vacland': 6, 'rem': 5, np.nan: 4}
def rad_landuse_classifier_many(uses):
    # RAD LAND USE CLASSIFICATION: reconciling [many] ATTOM <> [many] REGRID
    c = Counter(uses).most_common()
    # note: negative signs! descending by counts, then descending by SORT_ORDER (for breaking tie)
    landuse_orders = sorted(c, key = lambda x: (-x[1], -SORT_ORDER[x[0]]))
    for value, count in landuse_orders:
        if value not in ('vacland', 'rem', np.nan):
            return value
    if 'vacland' in set([k[0] for k in landuse_orders]): 
        return 'vacland'
    return 'rem'

def addr_fields_join(*args):
    args = [str(i).strip() for i in args]
    return " ".join(i for i in args if (i and i != 'nan' and i != 'None'))
addr_fields_join_vectorize = np.vectorize(addr_fields_join)


# only keep alphanumeric values; use to clean:
# ['LegalDescription', 'ParcelNumberRaw'] and ['legaldesc', 'parcelnumb']
keepchars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
del_chars = ''.join(c for c in map(chr, range(1114111)) if not c in set(keepchars))
# convert lowercases to uppercases
legal_number_translate_table = str.maketrans(keepchars, keepchars.upper(), del_chars)


government = ["Government", "Justice", "Library", "Department", "Administration","Dept of", "City of", "Dept of", 
              "Town of", "Commission", "National Foundation",  "Child Abuse", "Courthouse", "Department of ", 
              "Leadership", "Authority","Adoptions", "Prison","Veteran", "Chamber of", "Municipal","Child Support"]
religious = ["Church", "Baptist","Baptist Association","Christ","Family Harvest"]
community = ["Communications Workers Of America", "Veteran", "Community Center", "Community"]
education = ["High School", "Elementary School", "Education","College", 
                    "University", "Pre-School","Middle School", "School"]
outdoor = ["Garden", "Zoo", "Park", "Recreation", "Stadium", "Memorial", 
           "Outlook", "Overlook", "Pavilion", "Square", "Field", "Resort"]

stopcats = ['PERSONAL SERVICES',  'EATING - DRINKING', 'SHOPPING', 
       'AUTOMOTIVE SERVICES',  'BANKS - FINANCIAL', 'PET SERVICES']
keywords = {
    "health": ["Medical Center","Hospital","Clinic","Family Care", 
                 # "Rehab"
                ],
    "education": education, "government": government, "community": community, 
    "religious": religious, "outdoor": outdoor,
}

STOP_PARTS = ["LLC", "Llc", "inc", "Inc", "INC"]
stopwords = {
    "health":     STOP_PARTS + ["Hospitality", "Hospitalities", "Schools", "Animal", "Pet"],
    "education":  STOP_PARTS + ["Hospital"],
    "government": STOP_PARTS + ["Department Store"],
    "community":  STOP_PARTS,
    "religious":  STOP_PARTS,
    "outdoor":    STOP_PARTS,
}
caikeys = {
    "health": ["Medical Center","Hospital","Family Care"],
    "education": education, "government": government, "community": community, 
    "religious": religious, "outdoor": outdoor,
}

def getcai(dfinput, category):
    #STEP 1. use the name key word
    df = dfinput[dfinput["BUSNAME"].str.contains("|".join(keywords[category]))]
    #2.  remove the ones that contain the stop word
    df = df[~df["BUSNAME"].str.contains("|".join(stopwords[category]))]
    df = df[~df["CATEGORY"].isin(stopcats)]
    # 3. Find the CAI
    caidf = df[df["BUSNAME"].str.contains("|".join(caikeys[category]))]
    return caidf, df


def init():
    sf = SF57R[sf_code]
    list_of_feathers = glob(f"attom/data/attom-feather/{sf_code.upper()}_*.ftr")#[-1:]
    print(sf_code, "number of attom ftr chunks ", len(list_of_feathers)) 
    with Pool(16) as pool:
        attom = pool.map(lambda feather_file: pd.read_feather(feather_file, columns=attom_sel_cols), list_of_feathers)
        attom = pd.concat(attom, ignore_index=True)#.head(200)
    attom = attom.rename(columns= {
        '[ATTOM ID]': 'aid',
        'PropertyAddressFull' : 'Address',
        'LegalDescription' : 'Legal', 
        'ParcelNumberRaw' : 'Numb', 
        'PartyOwner1NameFull' : "Owner",
        'PartyOwner2NameFull' : 'Owner2',
        'PropertyAddressCity' : "ACity",
        'PropertyAddressZIP' : 'AZip',
        'PropertyLatitude' : 'Lat',
        'PropertyLongitude' : 'Lon', 
    })
    attom.shape # (3_330_480, 18) in 15sec; DC = (213_079, 18) 2sec
    # Address1 and AddressSub columns: 10sec
    attom['Address1'] = addr_fields_join_vectorize(attom.PropertyAddressHouseNumber,
        attom.PropertyAddressStreetDirection,
        attom.PropertyAddressStreetName,
        attom.PropertyAddressStreetSuffix,
        attom.PropertyAddressStreetPostDirection)

    attom['AddressSub'] = addr_fields_join_vectorize(attom.PropertyAddressUnitPrefix,
        attom.PropertyAddressUnitValue,)

    for col in ['Legal', 'Numb']:
        attom[col] = attom[col].str.translate(legal_number_translate_table)

    for bad_value in ["", "None"]:
        attom.replace(bad_value, np.nan, inplace = True)
    attom.fillna(np.nan, inplace=True)  # replace python NoneType with np.nan

    cols = ["Lat", "Lon"]
    attom[cols] = attom[cols].astype(float)

    # Hotfix bad raw data = If Address1 null, set addr = address1
    attom['Address1'] = np.where(
        attom.Address.notna() & attom.Address1.isna(), 
        attom['Address'], attom['Address1'])
    # If addr1 = addr, set addr2 = empty
    attom['AddressSub'] = np.where(
        attom.Address == attom.Address1, 
        np.nan, attom['AddressSub'])


    # Sanity check: when Address is null, no other addr subfields exist!
    cols = [
        'Address','Address1', 'AddressSub',
        'PropertyAddressHouseNumber','PropertyAddressStreetDirection',
        'PropertyAddressStreetName','PropertyAddressStreetPostDirection',
        'PropertyAddressStreetSuffix','PropertyAddressUnitPrefix','PropertyAddressUnitValue',
    ]
    check_address = set(attom.query("Address != Address")[cols].count()) == {0} 
    if not check_address:
        print(f"ALERT: ATTOM {sf_code} df: when Address is null, no other addr subfields should exist!")


    list_of_regrid_feathers = glob(f"attom/data/regrid-feather/{sf_code.lower()}_*.ftr")#[-1:]
    print(sf_code, "number of regrid ftr chunks ", len(list_of_regrid_feathers))
    with Pool(16) as pool:
        regrid = pool.map(lambda feather_file: pd.read_feather(feather_file, columns=regrid_sel_cols), list_of_regrid_feathers)
        regrid = pd.concat(regrid, ignore_index=True)#.head(200) 
    # when sf_code = DC: no parcel has legaldesc
    regrid[['legaldesc', 'parcelnumb']] = regrid[['legaldesc', 'parcelnumb']].astype(str) 

    regrid.rename(columns={'ll_uuid' : 'rid', 'sunit' : 'addressSub'}, inplace=True)
    regrid.fillna(np.nan, inplace=True) 

    # Fix small Bad data#1: paste address2 to adress (7, 17)
    regrid['address'] = np.where(regrid['address'].isna() & regrid['address2'].notna(),
                                 regrid['address2'], regrid['address'])
    mask = regrid.address.isin(['NO ADDRESS ASSIGNED BY COUNTY', 'No Situs Address'])
    # Fix bad bad data#2: hardcoded empty addresses
    regrid.loc[mask, 
        ['address', 'addressSub', 'saddno', 'address2',  
         'saddpref', 'saddstr', 'saddsttyp', 'saddstsuf' ]] = np.nan
    # regrid.shape # (3330480, 18) in 15sec ## NEW as of Jun3, (3_219_695, 17); DC = (137_403, 17)

    # Assemble REGRID address fields
    regrid['address1'] = addr_fields_join_vectorize(
        regrid.saddno, regrid.saddpref, regrid.saddstr, regrid.saddsttyp, regrid.saddstsuf)

    regrid.address1.replace("", np.nan, inplace=True)
    # Hotfix inconsistent address fields
    regrid.loc[regrid.address.isna() & regrid.address1.notna(), "address"] = regrid.address1
    regrid.loc[regrid.address.notna() & regrid.address1.isna() & regrid.address1.isna(), 
              ['address', 'address1', 'addressSub',]] = np.nan

    regrid['szip'] = regrid.szip.str.replace(r'\D+', '', regex = True).str[:5]
    regrid.szip.mask(regrid.szip.str.len() != 5, inplace=True)

    # Sanity check: when Address is null, no other addr subfields exist!
    cols = ['address', 'address1', 'addressSub', 'saddno', 'saddpref', 
                        'saddstr', 'saddsttyp', 'saddstsuf']
    check_address = set(regrid.query("address != address")[cols].count()) == {0} 
    if not check_address:
        print(f"ALERT: REGRID {sf_code} df: when Address is null, no other addr subfields should exist!")

    # Coords should have float types
    cols = ['lat', 'lon',]
    regrid[cols] = regrid[cols].astype(float)

    for col in ['legaldesc', 'parcelnumb']:
        regrid[col] = regrid[col].str.translate(legal_number_translate_table)

    # At this point, legaldesc and parcelnumb could be ""
    regrid.replace("", np.nan, inplace=True)

    all_possible_bad_values = ["", " ", "null", "Null", "None", "NONE", "none", "nan", "Nan"] 
    for bad_value in all_possible_bad_values:
        regrid.replace(bad_value, np.nan, inplace = True)

    if regrid[['lat', 'lon']].isna().sum().sum() != 0:
        print(f"NOTE: {sf_code} regrid_df contains {regrid[['lat', 'lon']].isna().sum().mean()} null coords lat/lon")


    # ADD h3 HEX COLUMNS
    for res in [7, 13]:
        # res 7 = length=15, last 6 chars = ffffff; area~5 km2
        # MO 24sec
        attom[f'ahex{res}']= gen_h3_hex(attom['Lat'], attom['Lon'],  res)
        regrid[f'rhex{res}']= gen_h3_hex(regrid['lat'], regrid['lon'],  res)

    attom_no_coords = attom.groupby("ahex7").size()[0] # 6669
    if attom_no_coords:
        print(f"NOTE: ATTOM contains {attom_no_coords} null lat/lon rows, whose h3hex = '0'")

    # dup columns check
    if (set(attom.columns.duplicated()), set(regrid.columns.duplicated())) != ({False}, {False}):
        print("ALERT: attom/regrid contains duplicate column names")

    print(f"COMPLETED init() and generated 2 dfs: {regrid.shape=}, {attom.shape=}")
    return regrid, attom

def legal(attom, regrid):
    # MATCH 1 - uniq legal description
    # IMPORTANT: first, drop the duplicate values
    regrid_legal_uniq = regrid.drop_duplicates(subset='legaldesc', keep=False)
    attom_legal_uniq = attom.drop_duplicates(subset='Legal', keep=False)
    legal_matched = attom_legal_uniq.merge(regrid_legal_uniq, left_on = 'Legal', right_on = 'legaldesc', how = 'inner')
    del regrid_legal_uniq, attom_legal_uniq
    # MO (1_620_809, 42), as of Jun3 (1_604_138, 42)
    # DC (0, 42), because REGRID DC has no legaldesc
    legal_diff = pd.DataFrame().reindex_like(legal_matched)
    if len(legal_matched):
        # MO = 13sec
        # Create new column that compares owner <> Owner
        legal_matched['owner_compare'] = owner_compare(legal_matched, 'owner', 'Owner')
        legal_diff = legal_matched.query(
            "parcelnumb.notna() & Numb.notna() & parcelnumb != Numb" 
            "& address.notna() & Address.notna() & address != Address"
            "& address1.notna() & Address1.notna() & address1 != Address1"
            "& rhex7 != '0' & ahex7 != '0' & rhex7 != ahex7" 
            "& owner.notna() & Owner.notna() & (~owner_compare)"    
        , engine = "python")

        print("legal_diff shape ", legal_diff.shape) # (87, 45), as of Jun3 (102, 45)
        # legal_diff[[ 'owner', 'owner2',  'Owner', 'Owner2', 'address1', 'Address1', 'parcelnumb', 'Numb',
        #             ]].sample(3)    
    print(f"COMPLETED legal matching {sf_code}: {legal_matched.shape}")
    return legal_matched, legal_diff

def numb(attom, regrid, legal_matched, legal_diff):
    # MATCH 2 - uniq parcel number
    # attom2 and regrid2: all rows that have not been legal matched!
    attom2 = attom[~attom.aid.isin(set(legal_matched.aid))]
    regrid2 = regrid[~regrid.rid.isin(set(legal_matched.rid))]
    regrid_parcelnumb = regrid2.drop_duplicates(subset='parcelnumb', keep=False)
    attom_parcelnumb = attom2.drop_duplicates(subset='Numb', keep=False)
    num_matched = attom_parcelnumb.merge(regrid_parcelnumb, left_on = 'Numb', right_on = 'parcelnumb', how = 'inner')
    del regrid2, attom2, regrid_parcelnumb, attom_parcelnumb
    # jun3 ((3219695, 20), (3330480, 22), (1604138, 45), (1207963, 42))
    regrid.shape, attom.shape, legal_matched.shape, num_matched.shape
    
    num_diff = pd.DataFrame().reindex_like(num_matched)
    if len(num_matched):
        num_matched['owner_compare'] = owner_compare(num_matched, 'owner', 'Owner')
        # num_matched.query("owner_compare == False").shape # (2918, 45)

        num_diff = num_matched.query(
               "address.notna() & Address.notna() & address != Address" # (86667, 42)
               "& address1.notna() & Address1.notna() & address1 != Address1" 
                "& rhex7 != '0' & ahex7 != '0' & rhex7 != ahex7" 
                "& owner.notna() & Owner.notna() & (~owner_compare)"      
        , engine = "python")
        # print(f"{num_diff.shape=}") # (112, 45); jun3 (140, 45)

    # SAVE CHECKPOINTS    
    ! mkdir -p temp/{date}
    if len(legal_matched): legal_matched.reset_index(drop=True).to_feather(f'temp/{date}/{sf_code}_legal.ftr')
    if len(num_matched): num_matched.reset_index(drop=True).to_feather(f'temp/{date}/{sf_code}_num.ftr')
    if len(legal_diff): legal_diff.reset_index(drop=True).to_feather(f'temp/{date}/{sf_code}_legaldiff.ftr')
    if len(num_diff): num_diff.reset_index(drop=True).to_feather(f'temp/{date}/{sf_code}_numdiff.ftr')
    print(f"COMPLETED parcel-number matching {sf_code}: {num_matched.shape}")
    return num_matched, num_diff


def placekey_query(attom, regrid, legal_matched, legal_diff, num_matched, num_diff):
    # MATCH 3 - PLACEKEY API
    attom3 = attom[~attom.aid.isin(pd.concat([legal_matched.aid, num_matched.aid]))] 
    attom3.shape # (82080, 22)

    attom3 = attom[~attom.aid.isin(pd.concat([legal_matched.aid, num_matched.aid]))] # concat 2 series
    regrid3 = regrid[~regrid.rid.isin(pd.concat([legal_matched.rid, num_matched.rid]))] # concat 2 series
    # Inner-join columns
    attom3 = pd.concat([attom3, legal_diff, num_diff], join='inner')
    regrid3 = pd.concat([regrid3, legal_diff, num_diff], join='inner')
    # print("attom3 regrid3 including na-Address1", attom3.shape, regrid3.shape)

    # CONFIRMED separately: without address (or address1), there would be no pkwhat 
    # i.e. filter for parcels with non-empty addresses only (We dont need pkwhere, as it is just h3 @res10)
    attom3 = attom3[attom3.Address1.notna()]
    regrid3 = regrid3[regrid3.address1.notna()]
    # print("attom3 regrid3 with valid Address1: ", attom3.shape, regrid3.shape)
    shared_cols = set(attom3).intersection(set(regrid3))
    if len(shared_cols):
        print("ALERT: expected empty shared cols: ", shared_cols)

    # BOTTLE NECK!!!!!! 40 MIN for MISSOURI!
    # for paralleling api calls/network ops, use async
    print(f"STARTING QUERYING PLACEKEY API: {sf_code}, attom3 {len(attom3)}, and regrid3 {len(regrid3)}")
    pkA = pk_call(attom3, sf_code, attom_pk_maps, 'aid', pk_api1)
    # print(f"STARTING QUERYING PLACEKEY API: {sf_code}, regrid3 {len(regrid3)}")
    pkR = pk_call(regrid3, sf_code, regrid_pk_maps, 'rid', pk_api2)
    # Unfuture.result() is a blocking operation except ...
    # pkA = pkA_unfuture.result()
    # pkR = pkR_unfuture.result()
    # pkA.shape, pkR.shape # ((253555, 25), (170883, 23)); jun3 ((254_306, 25), (172_566, 23))
    return pkA, pkR



def assemble_placekey(pkA, pkR):
    placekey_df = pd.concat([pkA, pkR], ignore_index=True)
    placekey_df.shape, placekey_df.pkwhat.count() # ((85915, 45), 85915)
    # IMPORTANT: SAVE PLACEKEY responses!
    placekey_df.to_feather(f'placekeyed/{date}_{sf_code}.ftr')
    # remove unique placekey values (unique addresses)
    placekey_df = placekey_df[placekey_df.duplicated(subset=['placekey'], keep=False)].copy()
    placekey_df.replace("", np.nan, inplace=True)
    placekey_df.sort_values(['pkwhere', 'pkwhat'], inplace=True)
    print(f"End of Placekey API: reconciled {len(placekey_df)} addresses, \
     into {placekey_df.placekey.nunique()} different placekey groups")

    return placekey_df

def pip(attom, regrid, legal_matched, num_matched, placekey_df):
    print(f"Start PIP step for state {sf_code}")
    ## MATCH 4 - PIP: attom points in regrid polygons
    attom4 = attom[~attom.aid.isin(
        set(legal_matched.aid).union(set(num_matched.aid)).union(set(placekey_df.aid))
    )]
    regrid4 = regrid[~regrid.rid.isin(
        set(legal_matched.rid).union(set(num_matched.rid)).union(set(placekey_df.rid))
    )]
    # REGRID 3.2mil down to 150k;  ATTOM 3.3mil down to 360k
    # DC jun14: regrid 140k to 2k, attom 210k to 4k
    regrid.shape, attom.shape, regrid4.shape, attom4.shape, 

    # ATTOM GEOMETRY TYPE
    attom4_geo = gp.GeoDataFrame(
            attom4, geometry=gp.points_from_xy(attom4.Lon, attom4.Lat)).set_crs(CRS_REGRID_PARCELS)
    # REGRID GEOMETRY TYPE
    list_of_regrid_feathers = glob(f"attom/data/regrid-feather/{sf_code.lower()}_*.ftr")#[-1:] # read regrid wkt files
    with Pool(16) as pool:
        regrid_wkt = pool.map(
            lambda feather : pd.read_feather(feather, columns=['ll_uuid', 'geometry']), 
            list_of_regrid_feathers)
        regrid_wkt = pd.concat(regrid_wkt, ignore_index=True)    

    regrid_wkt.rename(columns={'geometry': 'geom', 'll_uuid': 'rid'}, inplace=True)    
    # convert pandas df to geopandas df: 1min
    regrid_wkt = gp.GeoDataFrame(regrid_wkt, 
                                  geometry=gp.GeoSeries.from_wkt(regrid_wkt.geom, crs = CRS_REGRID_PARCELS))
    regrid_wkt = regrid_wkt.drop(columns = 'geom')
    # USE regrid_wkt.merge here, in order to retain geopandas df and CRS 
    regrid4_geo = regrid_wkt.merge(regrid4, on = 'rid', how='right')
    regrid4.shape, regrid4_geo.shape, type(regrid4_geo)

    # SPATIAL JOIN
    # `inner`: drop anything that didn't contain-within
    # RETAIN df_left geometry (regrid parcel polygon)
    df_pip = regrid4_geo.sjoin(attom4_geo, how="inner", predicate='contains')
    df_pip['geom'] = df_pip.geometry.to_wkt()
    df_pip.shape
    return attom4, regrid4, df_pip

def h3hex(attom4, regrid4, df_pip):
    ## MATCH 5 - h3 hex @res13
    df_last = pd.concat([attom4[~attom4.aid.isin(df_pip.aid)], 
                        regrid4[~regrid4.rid.isin(df_pip.rid)]], ignore_index= True)
    #### hex13 col is union of rhex13 and ahex13
    df_last['hex13'] = np.where(df_last.rhex13.notna(), df_last.rhex13, df_last.ahex13)

    # THESE ARE DUPLICATES: repeated hex13
    df_h3_dup = df_last[df_last.duplicated(subset=['hex13'], keep=False)].copy()
    df_h3_dup = df_h3_dup.query('hex13 != "0"')

    # df_rem also contains df_rem_nocoords (below) 
    # For now, consider them as new parcels
    df_last = df_last[~df_last.hex13.isin(df_h3_dup.hex13)] # i.e. drop_duplicates(subset=['hex13'], keep=False)
    return df_h3_dup, df_last

def gen_attom_landuse():
    # ATTOM: state landuse df: maps aid to our standardized landuse
    with Pool(16) as pool:
        alanduse = pool.map(
            lambda feather_file : pd.read_feather(
                feather_file, columns=['[ATTOM ID]', 'PropertyUseGroup',  'PropertyUseStandardized',]), 
            glob(f"attom/data/attom-feather/{sf_code.upper()}_*.ftr"))
        alanduse = pd.concat(alanduse, ignore_index=True)    

    alanduse.columns = ['aid', 'group', 
                        'code', # TODO LATER: use to identify MDU / subclass CAI (into edu/health/gov/commu)
                       ]
    # HOTFIX: group has mixed cases (e.g. both Commercial and COMMERCIAL present in ATTOM raw data)
    alanduse['group'] = alanduse.group.str.upper()

    # re-mapping aid-landuse
    conditions = [ 
        alanduse.group.eq('AGRICULTURE / FARMING'),
        alanduse.group.eq('RESIDENTIAL'),    
        alanduse.group.eq('VACANT LAND'),    
        alanduse.group.eq('PUBLIC WORKS'),    
        alanduse.group.isin(['INDUSTRIAL', 'COMMERCIAL']), ]
    choices = ['farm', 'resi', 'vacland', 'CAI', 'biz']
    alanduse['alanduse'] = np.select(conditions, choices, default='rem')
    # alanduse.alanduse.value_counts()
    print(f"Complete generating attom landuse df for {sf_code}, {alanduse.shape}")
    return alanduse

def gen_regrid_landuse():
    # REGRID state landuse: maps rid to our standardized landuse
    with Pool(16) as pool:
        rlanduse = pool.map(
            lambda feather_file : pd.read_feather(
                feather_file, columns=['ll_uuid', 'lbcs_activity', 'lbcs_function', 'lbcs_structure', 'lbcs_site', ]), 
            glob(f"attom/data/regrid-feather/{sf_code.lower()}_*.ftr"))
        rlanduse = pd.concat(rlanduse, ignore_index=True)    

    rlanduse.columns = ['rid', 'acti', 'func', 'struc', 'site']
    # Optimistic: assume parcels with missing lbcs_site: has building onsite i.e. = 6500
    # WORKAROUND: .astype(float) regress and fill in NaN, NEED TO chain fillna(6500) before coerce to int
    rlanduse['site'] = rlanduse.site.fillna("6500").astype(float).fillna(6500).astype(int) // 1000
    # NOTE: With structure iff lbcs_site = 6k
    rlanduse[['acti', 'struc', 'func']] = rlanduse[['acti', 'struc', 'func']].fillna("0").astype(float).fillna(0).astype(int) // 1000
    rlanduse['rlanduse']= np.vectorize(regrid_landuse_classifier)(
        rlanduse['acti'], rlanduse['func'],  rlanduse['struc'], rlanduse['site'])
    # rlanduse.rlanduse.value_counts()
    print(f"Complete generating regrid landuse df for {sf_code}, {rlanduse.shape}")
    return rlanduse

def assemble_uniq_identifier_df(legal_matched, legal_diff, num_matched, num_diff, alanduse, rlanduse, placekey_df, df_h3_dup, df_pip, df_last):
    # Exclude legal_diff (which later had been reviewed in Placekey step) from legal_matched
    # similarly, exclude num_diff from num_matched
    uniq_identifier_matched = pd.concat([
        legal_matched[~legal_matched.index.isin(legal_diff.index)],
        num_matched[~num_matched.index.isin(num_diff.index)],], ignore_index = True)[rad_in_cols]
    uniq_identifier_matched = uniq_identifier_matched.merge(alanduse[['aid', 'alanduse']], on='aid', how='left')
    uniq_identifier_matched = uniq_identifier_matched.merge(rlanduse[['rid', 'rlanduse']], on='rid', how='left')
    if len(uniq_identifier_matched):
        uniq_identifier_matched['landuse'] = np.vectorize(rad_landuse_classifier_single)(uniq_identifier_matched['alanduse'], uniq_identifier_matched['rlanduse'])
    else:
        uniq_identifier_matched['landuse'] = "None"
    # NOTE: as of now, 2 businesses with different SUITES are mapped to same placekey!
    # TODO LATER: need to consider addressSub to expand a address1 into multiple addresses, if possible! (and AddressSub to expand Address1) 
    pk_group = placekey_df.copy(deep = True)[rad_in_cols + ['placekey']]
    # each row (keyed on hex13) comes from either REGRID(has rid), or ATTOM(has aid)
    h3_group = df_h3_dup.copy(deep = True)[rad_in_cols + ['hex13']]

    for GROUPBY, group_df in zip(['placekey', 'hex13'], [pk_group, h3_group]):
        group_df = group_df.merge(alanduse[[
            'aid', 'alanduse']], on='aid', how='left').merge(rlanduse[[
            'rid', 'rlanduse']], on='rid', how='left')
        group_df = group_df.groupby(GROUPBY).agg({col : list for col in rad_in_cols + ['alanduse', 'rlanduse']})
        # Combine 2 list columns with + operator
        if len(group_df):
            group_df['landuse'] = np.vectorize(rad_landuse_classifier_many)(group_df['alanduse'] + group_df['rlanduse'])
        else:
            group_df['landuse'] = "None"            
        # Assign back to variables
        if GROUPBY == 'placekey':    pk_group = group_df # (142625, 17)
        else:                        h3_group = group_df # (5002, 17)

    # each row (keyed on rid) contains 1 rid and 1 aid
    pip_group_prev = df_pip.copy(deep=True)[rad_in_cols]
    pip_group_prev = pip_group_prev.merge(alanduse[['aid', 'alanduse']], on='aid', how='left')
    pip_group_prev = pip_group_prev.merge(rlanduse[['rid', 'rlanduse']], on='rid', how='left')

    attom_pip_agg_cols = ['aid', 'Address1', 'AddressSub', 'Owner', 'Owner2', 'Lat', 'Lon']
    pip_group = pip_group_prev.groupby('rid').agg({ col : list for col in attom_pip_agg_cols + ['alanduse']})
    pip_group_prev.set_index('rid', inplace=True)
    # drop duplicate indices
    pip_group_prev = pip_group_prev[~pip_group_prev.index.duplicated(keep='first')][
        ['rlanduse', 'address1', 'addressSub', 'owner', 'owner2', 'lat', 'lon']] 
    # excludes rid, includes rlanduse: turn a regular column into a list column
    for col in pip_group_prev.columns:
        pip_group_prev[col] = pip_group_prev[col].map(lambda x: [x])  

    pip_group = pip_group.join(pip_group_prev, how = 'left') 
    if len(pip_group):
        pip_group['landuse'] = np.vectorize(rad_landuse_classifier_many)(pip_group['alanduse'] + pip_group['rlanduse'])  
    else:
        pip_group['landuse'] = "None"                    
    ### enchance / add more fields to RAD2
    # copy rid to rId
    uniq_identifier_matched['rId'] = uniq_identifier_matched[['rid']]
    # 5 new columns: pId, pAddress1, pAddressSub, pLat, pLon
    conds = uniq_identifier_matched['address1'].isna() & uniq_identifier_matched['Address1'].notna()
    conds_ndarr = np.tile(conds.values[:, None], 3) # repeat conds values 3 times
    uniq_identifier_matched[['pId', 'pAddress1', 'pAddressSub', ]] = np.where(conds_ndarr, 
                            uniq_identifier_matched[['aid', 'Address1', 'AddressSub', ]],
                            uniq_identifier_matched[['rid', 'address1', 'addressSub', ]],)   
    uniq_identifier_matched['pLat'] = np.where(uniq_identifier_matched['lat'].isna() & uniq_identifier_matched['Lat'].notna(), 
                                     uniq_identifier_matched['Lat'], uniq_identifier_matched['lat'])
    uniq_identifier_matched['pLon'] = np.where(uniq_identifier_matched['lon'].isna() & uniq_identifier_matched['Lon'].notna(), 
                                     uniq_identifier_matched['Lon'], uniq_identifier_matched['lon'])
    # Hotfix bad owner data: has owner2, but not owner!
    uniq_identifier_matched['owner'] = np.where(uniq_identifier_matched['owner'].isna(), uniq_identifier_matched['owner2'], uniq_identifier_matched['owner'])
    uniq_identifier_matched['Owner'] = np.where(uniq_identifier_matched.Owner.isna(), uniq_identifier_matched.Owner2, uniq_identifier_matched.Owner)
    # Select pOwner from regrid owner or attom Owner
    uniq_identifier_matched['pOwner'] = np.where(uniq_identifier_matched['owner'].notna(), uniq_identifier_matched['owner'], uniq_identifier_matched['Owner'])
    # ASIDE: #rows with no owners
    # uniq_identifier_matched.query('pOwner != pOwner')[['owner', 'owner2', 'Owner', 'Owner2', 'pOwner']].shape

    # pk_group ~ h3_group
    if len(pk_group):
        pk_group['fields'] =  np.vectorize(rad_fields_vec)(
            pk_group['rid'],    pk_group['aid'],    pk_group['address1'],    pk_group['addressSub'],    
            pk_group['Address1'],    pk_group['AddressSub'],    pk_group['lat'],    pk_group['lon'],    
            pk_group['Lat'],    pk_group['Lon'],    
            pk_group['owner'],    pk_group['owner2'],    pk_group['Owner'],    pk_group['Owner2'],)
        # SPLIT fields into subfields
        pk_group[rad_out_cols] = pk_group.fields.str.split("\t", expand=True).replace(["None", "nan"], np.nan)
    else:
        pk_group[rad_out_cols] = np.nan
    # pk_group ~ h3_group
    if len(h3_group):
        h3_group['fields'] =  np.vectorize(rad_fields_vec)(
            h3_group['rid'],    h3_group['aid'],    h3_group['address1'],    h3_group['addressSub'],    
            h3_group['Address1'],    h3_group['AddressSub'],    h3_group['lat'],    h3_group['lon'],    
            h3_group['Lat'],    h3_group['Lon'],    
            h3_group['owner'],    h3_group['owner2'],    h3_group['Owner'],    h3_group['Owner2'],)
        # SPLIT fields into subfields
        h3_group[rad_out_cols] = h3_group.fields.str.split("\t", expand=True).replace(["None", "nan"], np.nan)
    else:
        h3_group[rad_out_cols] = np.nan 

    # pip_group is slightly different from (pk_group ~ h3_group)
    pip_group.reset_index(inplace=True)
    pip_group['rid'] = pip_group['rid'].map(lambda cell: [cell])

    if len(pip_group):
        pip_group['fields'] =  np.vectorize(rad_fields_vec)(
            pip_group['rid'],    pip_group['aid'],    pip_group['address1'],    pip_group['addressSub'],    
            pip_group['Address1'],    pip_group['AddressSub'],    pip_group['lat'],    pip_group['lon'],    
            pip_group['Lat'],    pip_group['Lon'],    
            pip_group['owner'],    pip_group['owner2'],    pip_group['Owner'],    pip_group['Owner2'],)
        # SPLIT fields into subfields
        pip_group[rad_out_cols] = pip_group.fields.str.split("\t", expand=True).replace(["None", "nan"], np.nan)
    else:
        pip_group[rad_out_cols] = np.nan

    ### df_last (all remaining uniq demand points)
    df_points = df_last.query('aid == aid')[
        ['aid', 'Owner', 'Owner2', 'Address1', 'AddressSub', 'Lat', 'Lon' ,]]
    df_points = df_points.merge(alanduse[['aid', 'alanduse']], 
                                on='aid', how='left').rename(columns= {
        'aid' : 'pId', 'alanduse' : 'landuse', 'Address1' : 'pAddress1',
        'AddressSub' : 'pAddressSub', 'Lat' : 'pLat', 'Lon' : 'pLon',
    })

    df_points['pOwner'] = np.where(df_points.Owner.isna(), df_points.Owner2, df_points.Owner)
    df_points['rId'] = np.nan


    # regrid_geom.merge(...geometry) # if want kepler visual
    df_polys = df_last.query('rid == rid')[
        ['rid', 'owner', 'owner2', 'address1', 'addressSub', 'lat', 'lon']]
    df_polys = df_polys.merge(rlanduse[['rid', 'rlanduse']], 
                              on='rid', how='left').rename(columns= {
        'rid' : 'pId', 'rlanduse' : 'landuse',
        'address1' : 'pAddress1', 'addressSub' : 'pAddressSub',
        'lat' : 'pLat', 'lon' : 'pLon',
    })
    df_polys['pOwner'] = np.where(df_polys.owner.isna(), df_polys.owner2, df_polys.owner)
    df_polys['rId'] = df_polys['pId']

    df_points.shape, df_polys.shape 

    # # ASIDE: has coords, but no address
    # # Remained here because these points had not been mapped to any polygons in PIP step
    # df_points.query('pAddress1 != pAddress1 & pLat == pLat').shape # (26401, 10) jun3 (26728, 10)

    ### add alternative IDs
    # add alternative ids as a list column
    uniq_identifier_matched['altIds'] = list(zip(uniq_identifier_matched.rid, uniq_identifier_matched.aid))
    pk_group['altIds'] = pk_group[['rid', 'aid']].apply(lambda row: [v for l in row for v in l if v is not np.nan], axis=1)
    h3_group['altIds'] = h3_group[['rid', 'aid']].apply(lambda row: [v for l in row for v in l if v is not np.nan], axis=1)
    pip_group['altIds'] = pip_group[['rid', 'aid']].apply(lambda row: [v for l in row for v in l if v is not np.nan], axis=1)
    # these groups altIds is simply pId (as a list of length 1)
    df_points['altIds'] = df_points['pId'].map(lambda cell: [cell])
    df_polys['altIds'] = df_polys['pId'].map(lambda cell: [cell])


    outcols = rad_out_cols + ['altIds', 'landuse']
    rad2 = pd.concat([uniq_identifier_matched[outcols], 
                pk_group[outcols].reset_index(),
                h3_group[outcols].reset_index(),
                pip_group[outcols], # index=rid=rId=pId
                df_points[outcols],
                df_polys[outcols],])
    print(f"Completed assembling rad2 df for {sf_code}, {rad2.shape}")
    return rad2

def assemble_poi():    
    poi = pd.read_feather(f'temp/jun10_poi_{sf_code}.ftr')#.head(300) # read from a saved feather
    poi = poi.drop(columns=['STATENAME', 'STATE', 'INDUSTRY', 'FRANCHISE', 'PRIMARY', 'COUNTY3', 'OBID', 'GEO_MATCH_CODE_TEXT'])
    poi = poi.query("BUSNAME == BUSNAME").copy() # not nan
    poi['NAMESTREETZIP'] = poi['BUSNAME'].str.strip() + " @ " + poi['STREET'].str.strip() + " @ " + poi['ZIP'].str.strip()
    poi = poi.drop_duplicates(["NAMESTREETZIP"], keep='first').reset_index(drop=True)
    # poi.shape

    cai_health, health_df = getcai(poi, "health")
    cai_gov, gov_df = getcai(poi, "government")
    cai_comm, comm_df = getcai(poi, "community")
    cai_edu, edu_df = getcai(poi, "education")
    cai_relig, relig_df = getcai(poi, "religious")
    cai_outdoor, outdoor_df = getcai(poi, "outdoor")
    # Dont need, @yuan what are these for?
    del cai_health, cai_gov, cai_comm, cai_edu, cai_relig, cai_outdoor 

    poi["DPtype"] = "commercial"
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(outdoor_df["BUSNAME"]), "outdoor", poi["DPtype"])
    # religious POI belongs to community
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(relig_df["BUSNAME"]), "community", poi["DPtype"])
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(edu_df["BUSNAME"]), "education", poi["DPtype"])
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(comm_df["BUSNAME"]), "community", poi["DPtype"])
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(gov_df["BUSNAME"]), "government", poi["DPtype"])
    poi["DPtype"] = np.where(poi["BUSNAME"].isin(health_df["BUSNAME"]), "health", poi["DPtype"])
    poi["CAIsubtype"] = np.where(poi["DPtype"].isin(["community","education","government","health"]), poi["DPtype"], "notCAI")
    poi["DPtype"] = np.where(poi["CAIsubtype"] == 'notCAI', poi["DPtype"], "CAI")


    # ASSUMPTION: no 2 biz/POIs occupies same hexagon ~ 2x3meter 
    poi = poi.rename(columns={"LONGITUDE" : 'lng', 'LATITUDE' : 'lat', 'DPtype': 'landuse'})
    poi = poi.h3.geo_to_h3(POI_h3_res).reset_index()

    # GENERATE ID from other values in the row
    # concat string columns using sum(axis=1); then map to hash values, using hash function
    poi['pId'] = poi[['NAMESTREETZIP', 'h3_14']].sum(axis=1).map(hash)
    # confirm all unique id
    if not (poi.shape[0] == poi.pId.count()== poi.pId.nunique()): # ((286050, 18), 286050, 286050)
        print("ALERT: missing unique id")

    poi['landuse'] = poi['landuse'].replace({"commercial" : 'biz'})

    poi_keep_cols = ['pId', 'BUSNAME', 'PHONE', 
            'STREET', 'CITY', 'ZIP', 
            'lng', 'lat', 'SIC', 'CATEGORY', 
            'landuse', 'CAIsubtype', 'h3_14',]
    poi = poi[poi_keep_cols].rename(columns={'STREET': 'pAddress1', 'CITY': 'pCity', 'ZIP': 'pZip', 
                          'BUSNAME': 'pOwner', 'CATEGORY' : 'pIndustry', 'SIC': 'pSIC'})
    poi['source'] = 'POI'
    print(f"Completed assembling POI df for {sf_code}, {poi.shape}")
    return poi

def assemble_rad3(rad2, poi):    
    rad2_with_dup = pd.DataFrame(rad2).rename(columns={'pLat' : 'lat', 'pLon' : 'lng'})
    rad2_with_dup[['lat', 'lng']] = rad2_with_dup[['lat', 'lng']].astype(float)
    rad2_with_dup = rad2_with_dup.h3.geo_to_h3(POI_h3_res).reset_index()
    rad2_with_dup['CAIsubtype'] = np.where(rad2_with_dup['landuse'] == 'CAI', 'parcelCAI', 'notCAI')
    rad3_with_dup = pd.concat([poi, rad2_with_dup], ignore_index=True)
    uniq_poi = rad3_with_dup.drop_duplicates(subset=[f'h3_{POI_h3_res}'], keep=False, ignore_index=True)
    uniq_poi = uniq_poi.query('source == "POI"')
    rad3 = pd.concat([uniq_poi, rad2_with_dup], ignore_index=True)
    rad2.shape, poi.shape, rad3.shape, # ((139663, 13), (48758, 14), (142533, 20))
    # 3_320_097 + 286_050 + h3 deduped = 3_389_890

    ### JOINING RAD3 with TIGER2019/CB
    tiger = pd.read_feather(f'tigerCB2019/{sf_code}.ftr')
    tiger.columns = ['GEOID', 'geom']
    # convert pandas df to geopandas df
    tiger = gp.GeoDataFrame(tiger, 
              geometry = gp.GeoSeries.from_wkt(tiger.geom, crs = CRS_REGRID_PARCELS))

    # convert pandas df to geopandas df
    rad3CB = gp.GeoDataFrame(rad3, 
               geometry = gp.points_from_xy(rad3.lng, rad3.lat, crs = CRS_REGRID_PARCELS))

    rad3CB = gp.sjoin(rad3CB, tiger, how="left", 
        predicate='within').rename(columns={'geom': 'wktCB'}).drop(
        columns=['index_right', 'hex13', 'geometry']).replace('nan', np.nan)

    print(f"NOTE: number of demand points fallen outside of CB boundaries = \
     {rad3CB.query('GEOID != GEOID').shape[0]}")


    ### ADD SPEED RANK
    date_download_ES = "jun14"
    ESdf = pd.read_feather(f'ESrank/{date_download_ES}_{sf_code}.ftr').rename(columns={
                                'speedRankReadyRaw': 'rank'})
    # dont need demand points fallen outside state censusblock boundaries
    rad3CB_rank = rad3CB.dropna(subset='GEOID').merge(ESdf[['GEOID', 'rank']], on='GEOID', how='left')

    rad3CB_rank.shape # (3386316, 22)

    ## Complete! Now SAVE TO MBTILES
    rad3CB_rank['pId'] = rad3CB_rank['pId'].astype(str)
    
    print(f"Completed RAD3 df for {sf_code}: {rad3CB_rank.shape}")
    # save to ftr
    rad3CB_rank.drop(columns=['h3_14', 'wktCB',]).to_feather(f'rad3_complete/{date}_{sf_code}.ftr')
    # save to csv
    rad3CB_rank.drop(columns=['h3_14', 'wktCB', 'placekey']).rename(
        columns={'rank': 'speedRankReadyRaw'}).to_csv(
        f'rad3_complete/{date}_{sf_code}.csv', index=False)

    with open('tippecanoe.cmds', 'a') as wf:
        wf.write(f"TIPPECANOE COMMAND for {sf_code}::: \n")
        cmd = f"/usr/local/bin/tippecanoe -zg -Z6 --extend-zooms-if-still-dropping --drop-densest-as-needed --force -o \
    /home/nhat/update-regrid/data/rad3_{sf_code.lower()}.mbtiles -l rad3_{sf_code.lower()} \
    /home/nhat/demand-points/rad3_complete/{date}_{sf_code}.csv \n"
        wf.write(cmd)

    return rad3CB_rank


# GLOBAL SCOPE    
POI_h3_res = 14  
rad_in_cols = ['aid', 'rid', 
              'address1', 'addressSub', 'Address1', 'AddressSub',
              'owner', 'owner2', 'Owner', 'Owner2', 
              'lat', 'lon', 'Lat', 'Lon', 
               # 'landuse',  
              ]
rad_out_cols = ['pId', 'rId', 'pAddress1', 'pAddressSub', 'pOwner', 'pLat', 'pLon']

def main(sf_code):
    regrid, attom = init()
    legal_matched, legal_diff = legal(attom, regrid)
    num_matched, num_diff = numb(attom, regrid, legal_matched, legal_diff)
    pkA, pkR = placekey_query(attom, regrid, legal_matched, legal_diff, num_matched, num_diff)
    print("COMPLETED querying Placekey API")
    placekey_df = assemble_placekey(pkA, pkR)
    attom4, regrid4, df_pip = pip(attom, regrid, legal_matched, num_matched, placekey_df)
    df_h3_dup, df_last = h3hex(attom4, regrid4, df_pip)

    # PART II: combine, reconcile, enhance RAD, and add ATTOM POI = RAD3
    alanduse = gen_attom_landuse()
    rlanduse = gen_regrid_landuse()
    #### add landuse to different groups
    rad2 = assemble_uniq_identifier_df(legal_matched, legal_diff, num_matched, num_diff, alanduse, rlanduse, placekey_df, df_h3_dup, df_pip, df_last)
    attom.shape, regrid.shape, rad2.shape # (3316869, 10); jun3 (3320097, 11)

    # ADD SOURCE COLUMN
    # len of pId==36 = from Regrid, under10 = from Attom
    print(f"diff lengths of pIds (REGRID vs ATTOM source): {set(rad2.pId.str.len())}") # {6, 7, 8, 9, 36}
    # REGRID = primary info from REGRID
    # ATTOM>REGRID = both REGRID and ATTOM cover, but ATTOM has more info
    # ATTOM = new points from ATTOM (REGRID miss-coverage)
    rad2['source'] = np.where(rad2.pId.str.len() == 36, 
                              'REGRID', 
                              np.where(rad2.rId.notna(), 'ATTOM>REGRID', 'ATTOM'))
    #### SAVING a big checkpoint: RAD2
    rad2.reset_index(drop=True).astype(str).to_feather(f'temp/{date}_{sf_code}_rad2.ftr')

    ## ADD ATTOM POI
    poi = assemble_poi()
    # add h3_14 column
    rad3CB_rank = assemble_rad3(rad2, poi)
    print(f"COMPLETE {sf_code}, {rad3CB_rank.shape}")


# call the function    
main(sf_code)



# STARTING QUERYING PLACEKEY API: DC, attom3 82015, and regrid3 5487 =>>  SHOULD TAKE 9 min!
# STARTING QUERYING PLACEKEY API: CA, attom3 2188058, and regrid3 1841795 =>> took 13min to reach this step: now PK API should take  220min i.e. 3hrs
# STARTING QUERYING PLACEKEY API: TX, attom3 3098185, and regrid3 272728 =>> took 15min to reach this step: now PK API should take 310min i.e. 5hrs
# STARTING QUERYING PLACEKEY API: RI, attom3 422208, and regrid3 328219 =>> should take 45min (actual: 1h30min total)