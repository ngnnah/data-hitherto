#!/bin/bash

# CURRENT crontab -l: everyday at 03:06 am
# 6 3 * * * bash /home/nhat/update-regrid/cron.bash >> /home/nhat/update-regrid/cron.log 2>&1

source /home/nhat/anaconda3/bin/activate py39;
ulimit -n `ulimit -Hn`;

echo "========== START-cron.bash $(date '+%D %X') 
conda info -e: $(conda info -e)";
cd /home/nhat/update-regrid/;
echo "Currently inside dir: $(pwd)";


# QUICK UPDATE SCAN: Find and save new changes to files 
# python3 "find_changes.py" >> find_changes.log && echo "Completed running script finding_changes.py ";

# FIRST THING FIRST: unzip: soft on CPU and RAM, can launch many jobs
# /home/nhat/bin/parallel --verbose -j900 "unzip -o {} -d /home/nhat/regrid-bucket/" :::: updated_zips.txt >> unzip.log && echo COMPLETE unzipping;


# (SEPARATELY) => CONCURRENTLY :

# 1 or 4. TIPPECANOE parallel: re-tile mbtiles: SUPER cpu intensive and slow, 1 job = 100%cpu => try to start this task as early as possible
# This is also least important job, so can do it as the final step
bash mbtiles.bash >> mbtiles.log 2>&1 &


# Temp pause
# 2. IJSON parallel: stream parsing updated_counties with ijson, and spatial join point-in-polygon and upload to ES
# bash parcel.bash >> parcel.log 2>&1 &

# Temp pause
# 3. OGR2OGR parallel: update POSTGIS db: delete, then upload
# bash postgis.bash >> postgis.log 2>&1 &


echo "END-cron.bash $(date '+%D %X') ==========";




