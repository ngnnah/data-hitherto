#!/bin/bash
source /home/nhat/anaconda3/bin/activate py39;
cd /home/nhat/speed;
time=$(date '+%F@%H:%M');


# # STEP 1.A
# nohup /home/nhat/bin/parallel --verbose -j15 --memfree 9G --memsuspend 7G "python3 ESdown.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &


# # STEP 1.B: Run ookla.ipynb first
# nohup /home/nhat/bin/parallel --verbose -j15 --memfree 9G --memsuspend 7G "python3 ookla_gen_tiles.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &


# # STEP 2
# nohup /home/nhat/bin/parallel --verbose -j15 --memfree 9G --memsuspend 7G "python3 ookla_ntia.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &


# # STEP 2.3; before Step2.5 upload 2021Q4 --25min to run: per Yuan request: download current speedCat values from bossdata, for comparison purposes
# nohup /home/nhat/bin/parallel --verbose -j3 --memfree 6G --memsuspend 6G "python3 ESdown_speed.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &

# # STEP 2.5 
# nohup /home/nhat/bin/parallel --verbose -j52 --memfree 9G --memsuspend 7G "python3 upload_speed_test.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &

# # STEP 3
# nohup /home/nhat/bin/parallel --verbose -j52 --memfree 9G --memsuspend 7G "python3 upload_bossdata.py {}" ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy >> cmds.log 2>&1 &


process_id=$!
wait $process_id
echo "Exit status: $?" >> cmds.log
echo " ========= STARTING @ ${time}  ========= "  >> cmds.log
echo " ========= COMPLETE @ $(date '+%F@%H:%M')  ========= " >> cmds.log