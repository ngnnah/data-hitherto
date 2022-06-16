# Filtering geojson, only keep features with non-null address
# SKIP gigabyte jsons counties
declare -A gb_counties=(                            
	[/home/nhat/regrid-bucket/az_maricopa.json]=1    
	[/home/nhat/regrid-bucket/ca_los-angeles.json]=1    
	[/home/nhat/regrid-bucket/ca_san-diego.json]=1    
	[/home/nhat/regrid-bucket/fl_broward.json]=1    
	[/home/nhat/regrid-bucket/il_cook.json]=1    
	[/home/nhat/regrid-bucket/tx_dallas.json]=1    
	[/home/nhat/regrid-bucket/tx_harris.json]=1    
) &&
commands_file="/home/nhat/jq_parallel_commands.txt";
# REMOVE existing commands file
rm -f $commands_file;
# BUG ALERT: CAREFUL OF THE whitespaces/tabs: https://unix.stackexchange.com/a/171498
for i in $(seq 52); do
  state=$(awk "NR==${i}{ print; exit }" ~/extras/state52.txt) &&
  echo $state &&
  jsons=$(awk "NR==${i}{ print; exit }" ~/extras/counties.txt) &&
  echo $jsons &&
  for json in $jsons; do
    output="$(basename -- $json)" &&
    # Retain geojson wrapper:: "jq '{features: [(.features  | map(. |= {properties}) | map(select(.properties.address != null))) | map(.properties |= {census_blockgroup, lat, lon, lbcs_function}) ] }' ${json} > ~/jsons/${output} ;"
    # JSONL (orient= records or typ=dictionary)
    [[ -n "${gb_counties[$json]}" ]] && echo "skipping ${json}" || \
      echo "jq -c '[ .features[] | select(.properties.address != null) | {cbg: .properties.census_blockgroup, lat: .properties.lat, lon: .properties.lon, func: .properties.lbcs_function}]' ${json} > ~/jsons/${output} ;" \
	    >> $commands_file;
  done
done
