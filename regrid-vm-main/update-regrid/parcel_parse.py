import sys
import ijson.backends.yajl2_c as ijson
import pandas as pd


REGRID_PARCEL_COLS = ['geoid', 'sourceagent', 'parcelnumb', 'usecode', 'usedesc', 'zoning', 'zoning_description', 'struct', 'multistruct', 'structno', 'yearbuilt', 'numstories', 'numunits', 'structstyle', 'parvaltype', 'improvval', 'landval', 'parval', 'agval', 'saleprice', 'saledate', 'taxamt', 'owntype', 'owner', 'ownfrst', 'ownlast', 'owner2', 'owner3', 'owner4', 'subsurfown', 'subowntype', 'mailadd', 'mail_address2', 'careof', 'mail_addno', 'mail_addpref', 'mail_addstr', 'mail_addsttyp', 'mail_addstsuf', 'mail_unit', 'mail_city', 'mail_state2', 'mail_zip', 'mail_urbanization', 'mail_country', 'address', 'address2', 'original_address', 'saddno', 'saddpref', 'saddstr', 'saddsttyp', 'saddstsuf', 'sunit', 'scity', 'city', 'county', 'state2', 'szip', 'urbanization', 'location_name', 'address_source', 'legaldesc', 'plat', 'book', 'page', 'block', 'lot', 'neighborhood', 'subdivision', 'qoz', 'qoz_tract', 'census_block', 'census_blockgroup', 'census_tract', 'census_school_district', 'sourceref', 'sourcedate', 'sourceurl', 'recrdareatx', 'recrdareano', 'gisacre', 'll_gisacre', 'sqft', 'll_gissqft', 'll_bldg_footprint_sqft', 'll_bldg_count', 'reviseddate', 'path', 'll_stable_id', 'll_uuid', 'll_updated_at', 'dpv_status', 'dpv_codes', 'dpv_notes', 'dpv_type', 'cass_errorno', 'rdi', 'usps_vacancy', 'usps_vacancy_date', 'cdl_raw', 'cdl_date', 'cdl_majority_category', 'cdl_majority_percent', 'padus_public_access', 'lbcs_activity', 'lbcs_activity_desc', 'lbcs_function', 'lbcs_function_desc', 'lbcs_structure', 'lbcs_structure_desc', 'lbcs_site', 'lbcs_site_desc', 'lbcs_ownership', 'lbcs_ownership_desc', 'lat', 'lon', 'taxyear', 'stacked_flag', 'll_last_refresh', 'll_address_count', 'homestead_exemption', 'alt_parcelnumb1', 'alt_parcelnumb2', 'alt_parcelnumb3', 'parcelnumb_no_formatting', 'plss_township', 'plss_section', 'plss_range' ]

# len(REGRID_PARCEL_COLS) # 129

filename_stem = sys.argv[1]
input_file = f'/home/nhat/regrid-bucket/{filename_stem}.json'
output_file = f'./processed-jsons/{filename_stem}.json'

with open(input_file, 'rb') as file:
    # https://github.com/ICRAR/ijson ; # properties is a yajl2 ~ a generator object
    properties = ijson.items(file, 'features.item.properties')
    # BIG NOTE: parallel parsing the json files often produce I/O read/write ERRORS
    # FIX: just find the files with issues, and try to parse them again!
    parcels = [(
                    prop['ll_uuid'],
                    prop['lat'], 
                    prop['lon'],
                    (prop['address'] if prop['address'] else '') + (prop['address2'] if prop['address2'] else ''),
                    prop['census_blockgroup'], 
                    prop['lbcs_activity'],
                    prop['lbcs_function'],
                    prop['lbcs_structure'],
                    prop['dpv_status'],            
                    prop['dpv_type'],            
                    prop['rdi'],            
                    prop['ll_bldg_count'],            
                    prop['ll_bldg_footprint_sqft']
                ) 
               for prop in properties]

    with open('parcel_counts.csv', 'a') as f:
        f.write(filename_stem.split('_')[0] + ',' + filename_stem + ',' + str(len(parcels)) + '\n')

    pd.DataFrame(parcels, columns= REGRID_PARCEL_COLS).to_json(output_file, orient='records')
    print(f'Complete parsing {filename_stem=} to {output_file=}')


