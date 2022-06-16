#!/bin/bash

# CURRENT crontab -l: "“At minute 0 past every 2nd hour.”" (every 2 hours)
# 0 */2 * * * bash /home/nhat/martin-cron.bash >> /home/nhat/martin-cron.log 2>&1
# test: every minute:
# * * * * * bash /home/nhat/martin-cron.bash >> /home/nhat/martin-cron.log 2>&1

source /home/nhat/anaconda3/bin/activate py39;
echo "************************";
time=$(date '+%F@%H:%M');
echo "START-cron.bash ${time}";

# # KILL ALL PROCESSES BY NAME martin
pgrep martin | xargs kill;

## martin PG dev-regrid @ port 3000 
# NGINX location ~ /parcels/(?<fwd_path>.*) >> proxy_pass    http://35.223.14.85:3000/$fwd_path$is_args$args;  
# http://35.223.14.85:3000/ 
# e.g. https://tiling.ready.net/parcels/public.parcels_ca.json  
nohup sh -c "export RUST_LOG=actix_web=info,martin=debug,tokio_postgres=debug && ~/martin --listen-addresses='0.0.0.0:3000' --watch --pool-size=80 --workers=32 postgres://boss_user:passDEV9g47uibjn2ijovZ@dev-regrid.db.ready.net:5432/boss_db_dev" > ~/martin-logs/${time}_regrid.nohuplog 2>&1  & 


## martin dev PG BOSS Users/subscribers/zones  @ port 4000
# BEFORE NGINX https redirect: proxy_pass: martin API endpoint: Table sources list @ http://35.223.14.85:4000/index.json , 
# e.g. http://35.223.14.85:4000/public.users.json ; http://35.223.14.85:4000/public.zones.json 
# AFTER NGINX https redirect: location ~ /boss/(?<fwd_path>.*) >> proxy_pass http://35.223.14.85:4000/$fwd_path$is_args$args;  e.g https://tiling.ready.net/boss/public.users.json or 
# http://35.223.14.85:4000/public.users.json
# http://35.223.14.85:4000/public.zones.json
nohup sh -c "export RUST_LOG=actix_web=info,martin=debug,tokio_postgres=debug && ~/martin --listen-addresses='0.0.0.0:4000' --watch --pool-size=80 --workers=32 postgres://boss_user:passDEV9g47uibjn2ijovZNEW@dev-boss.db.ready.net:5432/boss_db_dev" > ~/martin-logs/${time}_boss.nohuplog 2>&1  & 


echo "Both MARTIN tileservers (dev-regrid, and dev-boss) have been restarted. Now serving fresh data straight from PG";

echo "END-cron.bash $(date '+%F %X')"
echo "************************";



# DATABASE_URL=postgres://boss_user:passDEV9g47uibjn2ijovZ@dev-regrid.db.ready.net:5432/boss_db_dev cargo bench
# DATABASE_URL=postgres://boss_user:passDEV9g47uibjn2ijovZNEW@dev-boss.db.ready.net:5432/boss_db_dev cargo run




