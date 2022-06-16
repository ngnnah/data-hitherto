#!/bin/bash

source /home/nhat/anaconda3/bin/activate py39;

cd ~/users-table;

time1=$(date '+%F@%H:%M');

python3 "user-mbtiling.py";

# min-zoom = Z max-zoom = z 
/usr/local/bin/tippecanoe  -Z10 -z16 --extend-zooms-if-still-dropping --drop-densest-as-needed --force -o ~/update-regrid/data/users.mbtiles ./data/users.json;

docker restart tileserver;

time2=$(date '+%F@%H:%M');

echo "Start: ${time1} ; End: ${time2}" >> mbtiling.log




# https://github.com/mapbox/tippecanoe#cookbook
# --extend-zooms-if-still-dropping: If even the tiles at high zoom levels are too big, keep adding zoom levels until one is reached that can represent all the features
# --drop-densest-as-needed: If the tiles are too big at low or medium zoom levels, drop the least-visible features to allow tiles to be created with those features that remain
# -r1: Do not automatically drop a fraction of points at low zoom levels, since clustering will be used instead
# --cluster-distance=10: Cluster together features that are closer than about 10 pixels from each other
# --accumulate-attribute=POP_MAX:sum: Sum the POP_MAX (population) attribute in features that are clustered together. Other attributes will be arbitrarily taken from the first feature in the cluster.

# -aC or --cluster-densest-as-needed: If a tile is too large, try to reduce its size by increasing the minimum spacing between features, and leaving one placeholder feature from each group. The remaining feature will be given a "clustered": true attribute to indicate that it represents a cluster, a "point_count" attribute to indicate the number of features that were clustered into it, and a "sqrt_point_count" attribute to indicate the relative width of a feature to represent the cluster. If the features being clustered are points, the representative feature will be located at the average of the original points' locations; otherwise, one of the original features will be left as the representative.

# # point_count

# -r1 --cluster-distance=10 

# --cluster-densest-as-needed



docker run --rm -it -v $(pwd)/data:/data -p 8080:80 maptiler/tileserver-gl



ogr2ogr -f GeoJSON data/users.json PG:"host='dev-boss.db.ready.net' port='5432' user='boss_user' password='passDEV9g47uibjn2ijovZNEW' dbname='boss_db_dev'" -sql "SELECT account_status,account_type,first_name,id,last_name,map_point,service_status,updated_at,network_node_id FROM public.users" ID_GENERATE=YES


-lco ID_FIELD='id' 
OR
ID_GENERATE=YES


ogr2ogr -f GeoJSON users.json PG:"host='dev-boss.db.ready.net' port='5432' user='boss_user' password='passDEV9g47uibjn2ijovZNEW' dbname='boss_db_dev'" -sql "SELECT id,map_point,first_name,last_name,account_status,account_type, ... FROM public.users WHERE map_point IS NOT NULL"


cols = id,map_point,first_name,last_name,account_status,account_type,

sql = ... FROM public.users WHERE map_point IS NOT NULL


account_status,account_type,first_name,id,last_name,map_point,service_status,updated_at,network_node_id FROM public.users" ID_GENERATE=YES


ogr2ogr -f GeoJSON users11.json PG:"host='dev-boss.db.ready.net' port='5432' user='boss_user' password='passDEV9g47uibjn2ijovZNEW' dbname='boss_db_dev'" -sql "SELECT account_status,account_type,first_name,id,last_name,map_point,service_status,updated_at,network_node_id FROM public.users" -lco ID_FIELD='id'


--drop-densest-as-needed

tippecanoe -r1 --cluster-distance=10 --cluster-densest-as-needed -z16 --force -o users1.mbtiles users.json


tippecanoe -r1 --cluster-distance=10 -z16 --force -o users2.mbtiles users.json
tippecanoe -r1 --cluster-distance=10 -z16 --force -o users4.mbtiles users.json
tippecanoe --use-attribute-for-id=id -r1 --cluster-distance=10 --accumulate-attribute=id:concat -z16 --force -o users6.mbtiles users.json


tippecanoe -z16 -r1 --cluster-distance=30 --force -o data/users11.mbtiles users11.json
tippecanoe -z16 -r1 --cluster-distance=50 --force -o data/users12.mbtiles users11.json
  
  
  
/usr/local/bin/tippecanoe  -z16 -r1 --cluster-distance=60 --force -o ~/update-regrid/data/users.mbtiles ./data/users.json;



users_uf_demo.json


--cluster-distance=10 


tippecanoe -z16 -r1 --cluster-distance=50 --force -o data/ufdemo.mbtiles ufdemo.json

--accumulate-attribute=occupants_count:sum 
--accumulate-attribute=suspended_count:sum 
--accumulate-attribute=opportunity_count:sum 
--accumulate-attribute=lead_count:sum 
--accumulate-attribute=cancelled_count:sum 
--accumulate-attribute=subscriber_count:sum 
--accumulate-attribute=prospect_count:sum 
--accumulate-attribute=ineligible_count:sum 
--accumulate-attribute=pending_count:sum 
--accumulate-attribute=MDU_count:sum 
--accumulate-attribute=commercial_count:sum 
--accumulate-attribute=residential_count:sum 
--accumulate-attribute=null_acc_status_count:sum
--accumulate-attribute=null_acc_type_count:sum   
-y account_status
-y account_type
-y first_name
-y last_name
-y lat
-y lon

