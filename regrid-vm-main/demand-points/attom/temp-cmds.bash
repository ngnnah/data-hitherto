#!/bin/bash
source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/demand-points/attom;
ulimit -n `ulimit -Hn`;

time=$(date '+%F@%H:%M');



# # REGRID parse custom properties: 20 min + parcel polygon geometry only
# nohup /home/nhat/bin/parallel -j150 --memfree 15G --memsuspend 16G "python3 parse-regrid.py {}" :::: temp_stems_mo.txt >> temp_cmds.log 2>&1 &


# # Convert CSV to FEATHER for REGRID or ATTOM
nohup /home/nhat/bin/parallel -j10 --memfree 15G --memsuspend 17G "python3 csv2feather.py {}" ::: mo >> temp_cmds.log 2>&1 &


process_id=$!
wait $process_id
echo "Exit status: $?" >> temp_cmds.log
echo " ========= STARTING @ ${time}  ========= "  >> temp_cmds.log
echo " ========= COMPLETE @ $(date '+%F@%H:%M')  ========= " >> temp_cmds.log
