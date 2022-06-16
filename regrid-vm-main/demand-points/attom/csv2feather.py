import sys
from glob import glob
import pandas as pd

all_props = ['geoid', 'sourceagent', 'parcelnumb', 'usecode', 'usedesc', 'zoning', 'zoning_description', 'struct', 'multistruct', 'structno', 'yearbuilt', 'numstories', 'numunits', 'structstyle', 'parvaltype', 'improvval', 'landval', 'parval', 'agval', 'saleprice', 'saledate', 'taxamt', 'owntype', 'owner', 'ownfrst', 'ownlast', 'owner2', 'owner3', 'owner4', 'subsurfown', 'subowntype', 'mailadd', 'mail_address2', 'careof', 'mail_addno', 'mail_addpref', 'mail_addstr', 'mail_addsttyp', 'mail_addstsuf', 'mail_unit', 'mail_city', 'mail_state2', 'mail_zip', 'mail_urbanization', 'mail_country', 'address', 'address2', 'original_address', 'saddno', 'saddpref', 'saddstr', 'saddsttyp', 'saddstsuf', 'sunit', 'scity', 'city', 'county', 'state2', 'szip', 'urbanization', 'location_name', 'address_source', 'legaldesc', 'plat', 'book', 'page', 'block', 'lot', 'neighborhood', 'subdivision', 'qoz', 'qoz_tract', 'census_block', 'census_blockgroup', 'census_tract', 'census_school_district', 'sourceref', 'sourcedate', 'sourceurl', 'recrdareatx', 'recrdareano', 'gisacre', 'll_gisacre', 'sqft', 'll_gissqft', 'll_bldg_footprint_sqft', 'll_bldg_count', 'reviseddate', 'path', 'll_stable_id', 'll_uuid', 'll_updated_at', 'dpv_status', 'dpv_codes', 'dpv_notes', 'dpv_type', 'cass_errorno', 'rdi', 'usps_vacancy', 'usps_vacancy_date', 'cdl_raw', 'cdl_date', 'cdl_majority_category', 'cdl_majority_percent', 'padus_public_access', 'lbcs_activity', 'lbcs_activity_desc', 'lbcs_function', 'lbcs_function_desc', 'lbcs_structure', 'lbcs_structure_desc', 'lbcs_site', 'lbcs_site_desc', 'lbcs_ownership', 'lbcs_ownership_desc', 'lat', 'lon', 'taxyear', 'stacked_flag', 'll_last_refresh', 'll_address_count', 'homestead_exemption', 'alt_parcelnumb1', 'alt_parcelnumb2', 'alt_parcelnumb3', 'parcelnumb_no_formatting', 'plss_township', 'plss_section', 'plss_range' ]
regrid_header = all_props + ['geom']

sf_code = sys.argv[1] # e.g. TX or tx
sf_code = sf_code.lower()

regrid = []
for i in glob(f'data/regrid-csv/{sf_code}*.csv'):
    regrid.append(pd.read_csv(i, dtype=str, sep='\t',
                 names = regrid_header, # use all columns
                ))
if len(regrid):
    # need to ignore index/serialized index for to_feather to work
    regrid = pd.concat(regrid, ignore_index = True) 
    print(sf_code, regrid.shape)    # MO SHOULD BE: 90sec, (3330480, 19); de 13sec (431638, 130)
    regrid.to_feather(f'data/regrid-feather/{sf_code}.ftr')
else: 
    print("ALERT: no csv input files found for: ", sf_code)
