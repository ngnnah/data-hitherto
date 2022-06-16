#!/bin/bash

source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/update-regrid/;

echo "======START parallel tiling
with mbtiles tippecanoe , date of $(date '+%D %X')" 

# find, list, and write tippecanoe commands to monthly_tippecanoe.txt
/home/nhat/anaconda3/envs/py39/bin/python3 list_ndgeojson.py &&
# run tippecanoe commands inside monthly_tippecanoe.txt
/home/nhat/bin/parallel --verbose -j56 --memfree 6G --memsuspend 9G < monthly_tippecanoe.txt &

# https://linuxize.com/post/bash-wait/
process_id=$!
echo "PID: $process_id"
wait $process_id
echo "Exit status: $?"

# THEN, dont forget to restart tileserver-gl to serve newly refreshed static mbtiles
docker restart tileserver;
echo "Tilesever-gl docker has been restarted: ";

echo "COMPLETE parallel mbtiles tippecanoe tiling $(date '+%D %X') END======" 



