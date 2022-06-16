#!/bin/bash

source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/allocation/;

nohup sh -c "/home/nhat/bin/parallel --verbose -j6 --memsuspend 10G python3 es_download.py {} ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy" > es_download.nohuplog 2>&1  & 

# nohup sh -c "/home/nhat/bin/parallel --verbose -j6 --memsuspend 12G python3 es_download.py {} ::: co va vt" > es_download.nohuplog 2>&1  & 



