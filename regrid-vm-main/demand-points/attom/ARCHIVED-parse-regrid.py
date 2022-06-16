import sys
# import ijson.backends.yajl2_c as ijson
import json
from shapely.geometry import shape
import csv
import gzip

all_props = ['geoid', 'sourceagent', 'parcelnumb', 'usecode', 'usedesc', 'zoning', 'zoning_description', 'struct', 'multistruct', 'structno', 'yearbuilt', 'numstories', 'numunits', 'structstyle', 'parvaltype', 'improvval', 'landval', 'parval', 'agval', 'saleprice', 'saledate', 'taxamt', 'owntype', 'owner', 'ownfrst', 'ownlast', 'owner2', 'owner3', 'owner4', 'subsurfown', 'subowntype', 'mailadd', 'mail_address2', 'careof', 'mail_addno', 'mail_addpref', 'mail_addstr', 'mail_addsttyp', 'mail_addstsuf', 'mail_unit', 'mail_city', 'mail_state2', 'mail_zip', 'mail_urbanization', 'mail_country', 'address', 'address2', 'original_address', 'saddno', 'saddpref', 'saddstr', 'saddsttyp', 'saddstsuf', 'sunit', 'scity', 'city', 'county', 'state2', 'szip', 'urbanization', 'location_name', 'address_source', 'legaldesc', 'plat', 'book', 'page', 'block', 'lot', 'neighborhood', 'subdivision', 'qoz', 'qoz_tract', 'census_block', 'census_blockgroup', 'census_tract', 'census_school_district', 'sourceref', 'sourcedate', 'sourceurl', 'recrdareatx', 'recrdareano', 'gisacre', 'll_gisacre', 'sqft', 'll_gissqft', 'll_bldg_footprint_sqft', 'll_bldg_count', 'reviseddate', 'path', 'll_stable_id', 'll_uuid', 'll_updated_at', 'dpv_status', 'dpv_codes', 'dpv_notes', 'dpv_type', 'cass_errorno', 'rdi', 'usps_vacancy', 'usps_vacancy_date', 'cdl_raw', 'cdl_date', 'cdl_majority_category', 'cdl_majority_percent', 'padus_public_access', 'lbcs_activity', 'lbcs_activity_desc', 'lbcs_function', 'lbcs_function_desc', 'lbcs_structure', 'lbcs_structure_desc', 'lbcs_site', 'lbcs_site_desc', 'lbcs_ownership', 'lbcs_ownership_desc', 'lat', 'lon', 'taxyear', 'stacked_flag', 'll_last_refresh', 'll_address_count', 'homestead_exemption', 'alt_parcelnumb1', 'alt_parcelnumb2', 'alt_parcelnumb3', 'parcelnumb_no_formatting', 'plss_township', 'plss_section', 'plss_range' ]


filename_stem = sys.argv[1] # e.g. tx_loving

# NOTE the files' extension, and locations of files
# Read from bucket is awfully slow, and congested at time
# instead, read from the copied zips
ifile = f'/home/nhat/regrid-bucket/{filename_stem}.ndgeojson.gz'
ofile = f'data/regrid-csv/{filename_stem}.csv'

print(f'START parsing {ofile}')


with gzip.open(ifile, "r") as fin:
    count = 0
    # load line by line with json.loads (instead of ijson)
    for line in fin:
        line = json.loads(line)
        count += 1
        # PROPERTIES & GEOMETRY
        row = []
        properties = line.get('properties', {})
        geom = line.get('geometry', "")
        # TypeError: Object of type Decimal is not JSON serializable 
        # FIX: convert all other values/types (Decimal, etc.) to str
        for col in all_props:
            v = str(properties.get(col, "NONE")).strip()
            if v in {"", "None", "NONE"}:
                row.append("")
            else:
                row.append(v)
        
        if not geom:
            rid = properties.get('ll_uuid', "Error: no rid")    
            print(f"ALERT: {ifile}, line {count}, {rid=}: has no geom")
        else:
            # Convert to wkt: https://gist.github.com/drmalex07/5a54fc4f1db06a66679e
            geom = shape(geom).wkt   
        row.append(geom)
        # WRITE THE NEW LINE TO CSV
        with open(ofile, 'a', newline='') as csvfile:
            # pd.read_csv(quoting=0 QUOTE_MINIMAL)
            writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)  

    print(f'Complete parsing #{count} features for {ofile}')
