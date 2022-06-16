#!/bin/bash


# Fuse Bucket as a folder in Linux filesystem
gcsfuse --implicit-dirs boss-geo-data regrid-bucket;

source /home/nhat/anaconda3/bin/activate py39;

# DOCKER FORMAT: `host:container`
# -p (port) HOST-PORT:CONTAINER-PORT publish the container port () to the host port ()



# # restart all martin servers running in background
# echo "Executed ~/startup.bash" >> /home/nhat/martin-cron.log
# bash /home/nhat/martin-cron.bash >> /home/nhat/martin-cron.log 2>&1 &




## MBTILE tileserver-gl @ port 5000 (port 80 on container)
# -p 5000:80 Map TCP port 80 in the container to TCP port 5000 on the Docker host
# NGINX https://tiling.ready.net/data/ ; proxy_pass http://35.223.14.85:5000/data/;
# docker run -d --name tileserver -v $(pwd)/data:/data -p 5000:80 maptiler/tileserver-gl
# tileserver-gl / mbtiles: quick inspection @ http://35.223.14.85:5000/
# location /data >> proxy_pass http://35.223.14.85:5000/data/;
# e.g. https://tiling.ready.net/data/parcels_ca.json
# TRUNCATE ALL log files: sudo sh -c "truncate -s 0 /var/lib/docker/containers/*/*-json.log"
# CHECK logs: docker logs tileserver -f --tail 20 
docker restart tileserver; 
# VISIT file:///Users/nhatnguyen/Desktop/READY/regrid/index-addSource.html to confirm tilserver-gl is serving mbtiles


## OSRM @ port 7000 (port 5000 on container)
# Start routing engine HTTP server on port 7000
# i.e. publish the container port (5000) to the host port (7000) so the container can communicate with  the “outside world”. 
# NOTE: remember to allow HTTP connection (add Firewall rule) at tcp:7000
# docker run -d --rm -p 7000:5000 --name osrm -v "/home/nhat/allocation/building-to-road:/data" osrm/osrm-backend osrm-routed --algorithm mld /data/district-of-columbia-latest.osrm



## JUPYTERLAB @ port 8888
nohup sh -c "jupyter lab --ip 0.0.0.0 --port 8888 --no-browser" > ~/jupyter.nohuplog 2>&1  & 






## LATER: VROOM @ port 8000


# users mbtiles, tippecanoe >> tileserver-gl with docker @ port 9000
# docker run -d --name users -v $(pwd)/data:/data -p 9000:80 maptiler/tileserver-gl
# docker restart users


# SUMMARY: FIREWALL RULES: regrid-tiling-vm:  3000 (martin regrid), 4000 (martin boss), 5000 (docker tileserver-gl serving mbtiles), 8888 (jupyterlab), 7000 (OSRM), 8000 (VROOM), EXTRAS: 6000 (docker run -p 6000:80 failed!!), 9000 (mbtiles mdu/users testing)
