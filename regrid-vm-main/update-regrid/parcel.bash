#!/bin/bash

source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/update-regrid/;
ulimit -n `ulimit -Hn`;

# FIRST, parse LARGE geojson into bitesize jsons
echo "======START parcel_parse.py: 
streaming parsing $(cat updated_counties.txt | wc -l) counties, date of $(date '+%D %X')" 

# QUITE FAST: ~10min to stream parse 200 counties // CAUTION: I/O error occurs when -j is too high
/home/nhat/bin/parallel --memfree 2G --memsuspend 4G -j200 "python3 parcel_parse.py {}" :::: updated_counties.txt & 

# https://linuxize.com/post/bash-wait/
# https://unix.stackexchange.com/a/76721
process_id=$!
echo "PID: $process_id"
wait $process_id
echo "Exit status: $?"
echo "COMPLETE parcel_parse.py: ijson-stream-parsing $(date '+%D %X') END======" 



# AFTER parsing, execute point-in-polygon to calculate number of parcels per census block and upload to ES
echo "======START parcel_pip.py: 
point-in-polygon sjoin: number of parcel addresses per censusblock, for $(cat updated_states.txt | wc -l) states ($(cat updated_states.txt | xargs)), date of $(date '+%D %X')" 
# How many jobs can I parallel here?
# --memfree size
# Minimum memory free when starting another job
# --memsuspend size
# If the available memory falls below 2 * size, GNU parallel will suspend some of the running jobs. If the available memory falls below size, only one job will be running

# # # ::: $(head -6 updated_states.txt | tail -2) & 
# # # ::: fl ia id il mi
/home/nhat/bin/parallel --verbose -j7 --memfree 10G --memsuspend 9G "python3 parcel_pip.py {}" :::: updated_states.txt & 

process_id=$!
echo "PID: $process_id"
wait $process_id
echo "Exit status: $?"
echo "======COMPLETE parcel_pip.py: point-in-polygon sjoin $(date '+%D %X') END======" 



