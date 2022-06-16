import sys
import pandas as pd
from shapely.geometry import shape
from glob import glob
from multiprocess import Pool

REGRID_CHUNK_SIZE = 80_000
# Important: geometry and properties must have type dict (elif str: need further costly transformation), in order for .apply(shape) to work!
REGRID_DTYPE_MAP = {'geometry': dict, 'properties' : dict, 'id': int, 'type' : "category"}

def process_regrid_chunk(stem, chunk_order, chunk_df):
    # print(stem, chunk_order)
    df = pd.json_normalize(chunk_df.properties)
    # ERROR for tx_harris county: pyarrow.lib.ArrowTypeError: ("Expected bytes, got a 'list' object", 'Conversion failed for column property_name with type object')
    # WORKAROUND
    if 'property_name' in set(df): # NOT ALL ndgeojson contains this column
        df['property_name'] = df['property_name'].astype(str)
    # chunk_df.geometry currently has form: {'type': 'Polygon', 'coordinates': [[[-77.0978...
    # 800ms (chained apply() is 10x faster than vectorize!!!)
    geometry = chunk_df.geometry.apply(shape).apply(
        lambda row: row.wkt).reset_index(drop=True) 
    # geometry is now a series of wkt string type, with serial index
    df = pd.concat([df, geometry], axis=1)
    # print(sf, stem, chunk_order, df.shape)
    # df.shape, df.geometry.count(), type(df.geometry.loc[0])
    if len(df):
        name = f"{stem}_c{REGRID_CHUNK_SIZE}_{chunk_order}" # all lower cases
        df.to_feather(f'/home/nhat/demand-points/attom/data/regrid-feather/{name}.ftr')
        del df
                  
def process_sf(sf):
    sf = sf.lower()
    files = glob(f'/home/nhat/regrid-bucket/{sf}_*.ndgeojson.gz')
    # print(f"{sf}, list of county files: {files} \n")
    def func(ifile):
        stem = ifile.split('/')[-1].split('.')[0]
        chunks = pd.read_json(ifile, lines=True, chunksize=REGRID_CHUNK_SIZE, dtype=REGRID_DTYPE_MAP)
        for chunk_order, chunk_df in enumerate(chunks):
            process_regrid_chunk(stem, chunk_order, chunk_df[['properties', 'geometry']])
        print(f"Done processing {sf} state: county {ifile.split('/')[-1]}")
        
    with Pool(12) as pool:
        pool.map(func, files, chunksize=3)    
        
    print(f"Done processing {sf} state which includes {len(files)} counties")
    
    
sf = sys.argv[1] # e.g. sf
process_sf(sf)            
    
    