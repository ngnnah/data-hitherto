# nohup parallel --verbose --progress --resume-failed --joblog ./TEST3-script-upload.joblog -j15 -k < ./uploadCmds.txt > ./TEST3-script-upload.nohuplog 2>&1  &    

# nohup parallel --verbose --progress --resume-failed -j15 -k < ./uploadCmds.txt > ./TEST3-script-upload.nohuplog 2>&1  &    

# NEW UPLOAD, different from UPDATE Cmd
nohup sh -c "cat ./uploadCmds.txt | parallel --verbose --progress --results ./upload.resultslog -j52 -k --pipe -N1 bash" > ./upload.nohuplog 2>&1  & 


# f
# nohup parallel --verbose --progress --resume-failed --joblog ./TESTf-script-upload.joblog -j52 -k < ./f_uploadCmds.txt > ./TESTf-script-upload.nohuplog 2>&1  &    

# g
nohup parallel --verbose --progress --resume-failed --joblog ./g-script-upload.joblog -j52 -k < ./g_uploadCmds.txt > ./g-script-upload.nohuplog 2>&1  &    

# h
nohup parallel --verbose --progress --resume-failed --joblog ./h-script-upload.joblog -j52 -k < ./h_uploadCmds.txt > ./h-script-upload.nohuplog 2>&1  &   

nohup parallel --verbose --progress --resume-failed --joblog ./i.joblog -j52 -k < ./i_uploadCmds.txt > ./i.nohuplog 2>&1  &    
nohup parallel --verbose --progress --resume-failed --joblog ./j.joblog -j52 -k < ./j_uploadCmds.txt > ./j.nohuplog 2>&1  &    
nohup parallel --verbose --progress --resume-failed --joblog ./k.joblog -j52 -k < ./k_uploadCmds.txt > ./k.nohuplog 2>&1  &    
nohup parallel --verbose --progress --resume-failed --joblog ./l.joblog -j52 -k < ./l_uploadCmds.txt > ./l.nohuplog 2>&1  & 
nohup parallel --verbose --progress --resume-failed --joblog ./m.joblog -j52 -k < ./m_uploadCmds.txt > ./m.nohuplog 2>&1  & 
nohup parallel --verbose --progress -j52 < ./n_uploadCmds.txt > ./n.nohuplog 2>&1  & 
nohup parallel --verbose --progress -j52 < ./o_uploadCmds.txt > ./o.nohuplog 2>&1  & 

nohup parallel --verbose --progress -j52 < ./p_uploadCmds.txt > ./p.nohuplog 2>&1  &

nohup parallel --verbose --progress -j52 < ./q_uploadCmds.txt > ./q.nohuplog 2>&1  &

nohup bash ./r_uploadCmds.txt > ./r.nohuplog 2>&1  &
nohup bash ./s_uploadCmds.txt > ./s.nohuplog 2>&1  &
nohup bash ./a_uploadCmds.txt > ./a.nohuplog 2>&1  &



nohup sh -c "cat ./uploadCmds.txt | parallel --verbose -j150 --pipe -N1 bash" > ./a_upload.nohuplog 2>&1  & 
nohup sh -c "cat ./uploadCmds.txt | parallel --verbose -j150 --pipe -N1 bash" > ./b_upload.nohuplog 2>&1  & 
nohup sh -c "cat ./uploadCmds.txt | parallel --verbose -j150 --pipe -N1 bash" > ./c_upload.nohuplog 2>&1  & 


pgrep ogr2ogr | xargs kill


# https://gis.stackexchange.com/a/296749
# name of geometry field
# ogrinfo -sql "select * from ky_boone" /home/nhat/regrid-bucket/ky_boone.json
# Geometry Column = _ogr_geometry_

# ogrinfo -dialect sqlite -sql "select * from ky_boone" /home/nhat/regrid-bucket/ky_boone.json | head -20
# Geometry Column = GEOMETRY


# nohup $(echo "parallel --progress -j 52 python3 ./parcelCount.py {} '>' ./parcelCountLogs/{}.log ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy") > ./parcelCountLogs/_main.nohuplog 2>&1  & 
# nohup $(echo "parallel --progress --resume-failed --joblog ./parcelCount.joblog -j 52 python3 ./parcelCount.py {} > ./parcelCount.pylog ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy") > ./parcelCount.nohuplog 2>&1  & 
# parallel --verbose --progress -j 52 python3 ./parcelCount.py {} > ./parcelCountLogs/{}.log ::: ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy



ogr2ogr -f 'PostgreSQL' PG:'host=dev-regrid.db.ready.net port=5432 user=boss_user password=passDEV9g47uibjn2ijovZ dbname=boss_db_dev' -skipfailures -unsetFid -progress -append -nlt 'PROMOTE_TO_MULTI' '/home/nhat/regrid-bucket/ak_denali.json' -nln 'regrid.e_template' --config PG_USE_COPY YES ; 




