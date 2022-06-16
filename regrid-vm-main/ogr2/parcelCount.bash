#!/bin/bash
source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/ogr2/;
ulimit -n `ulimit -Hn`;

echo "======START parcel counting, date of $(date '+%D %X')" >> parcelCount.log

nohup $(echo "parallel -j 500 python3 parcelCount.py {} county :::: parcelCount-jsons_2021-12-22.txt") >> parcelCount.log 2>&1  & 

echo "======COMPLETE parcel counting, date of $(date '+%D %X') END======" >> parcelCount.log

# RUN: bash ~/ogr2/parcelCount.bash

# STATE
# nohup $(echo "parallel -j 52 python3 ./parcelCount.py {} state ::: \
# ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy") \
# > ./parcelCount-state.nohuplog 2>&1  & 


# parallel --verbose -j 500 python3 parcelCount.py {} county :::: nm-jsons.txt


