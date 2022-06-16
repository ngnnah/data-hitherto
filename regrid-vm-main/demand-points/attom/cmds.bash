#!/bin/bash
source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/demand-points/attom;
ulimit -n `ulimit -Hn`;

time=$(date '+%F@%H:%M');


# # REGRID ndgeojson.gz to feather
# TOTAL TIME: 2h10min
nohup /home/nhat/bin/parallel -j8 --memfree 18G --memsuspend 20G "python3 parse-regrid.py {}" ::: AK AL AR AS AZ CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MD ME MI MN MO MP MS MT NC ND NE NH NJ NM NV NY OH OK OR PA PR RI SC SD TN TX UM UT VA VI VT WA WI WV WY  >> cmds.log 2>&1 &

## Single state (failed ftr outputs) ~error: "cant be a proper file because too small ..."
# python3 ~/demand-points/attom/parse-regrid-multi.py TX
# python3 ~/demand-points/attom/parse-regrid-multi-txharris.py TX


process_id=$!
wait $process_id
echo "Exit status: $?" >> cmds.log
echo " ========= STARTING @ ${time}  ========= "  >> cmds.log
echo " ========= COMPLETE @ $(date '+%F@%H:%M')  ========= " >> cmds.log
