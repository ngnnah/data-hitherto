#!/bin/bash
cd /home/nhat/demand-points/attom/

# INSTRUCTION: Run this file by: bash demand-points/attom/attom-wget.bash &
# To get download urls: right click each individual zips, copy Link Address of Download (small square icon next to name)
# e.g. https://file.ac/api/3/link/9vTO_IBfOo8/READYDOTNET_TAXASSESSOR_0003.zip?download=true
# e.g. POI https://file.ac/api/3/link/5P8PidGbYP0/POI_202205.zip?download=true

time=$(date '+%F@%H:%M');

nohup /home/nhat/bin/parallel -j17 --memfree 9G --memsuspend 7G "wget {} -P /home/nhat/demand-points/attom/data/attom-zip/" ::: https://file.ac/api/3/link/phfhduZlt-U/READYDOTNET_TAXASSESSOR_0002_001.zip?download=true https://file.ac/api/3/link/-_4WFOXp9yo/READYDOTNET_TAXASSESSOR_0002_002.zip?download=true https://file.ac/api/3/link/hMC6CvY1zxI/READYDOTNET_TAXASSESSOR_0002_003.zip?download=true https://file.ac/api/3/link/AYaSIaQrfok/READYDOTNET_TAXASSESSOR_0002_004.zip?download=true https://file.ac/api/3/link/D-NIsg97Vhk/READYDOTNET_TAXASSESSOR_0002_005.zip?download=true https://file.ac/api/3/link/xf4irMCatrM/READYDOTNET_TAXASSESSOR_0002_006.zip?download=true https://file.ac/api/3/link/patsKe7CI-0/READYDOTNET_TAXASSESSOR_0002_007.zip?download=true https://file.ac/api/3/link/el3iZXafonw/READYDOTNET_TAXASSESSOR_0002_008.zip?download=true https://file.ac/api/3/link/fx8mUp-yPOk/READYDOTNET_TAXASSESSOR_0002_009.zip?download=true https://file.ac/api/3/link/PDsTmLipFG8/READYDOTNET_TAXASSESSOR_0002_010.zip?download=true https://file.ac/api/3/link/bCLPSoqDdXw/READYDOTNET_TAXASSESSOR_0002_011.zip?download=true https://file.ac/api/3/link/wkgtadD-DPY/READYDOTNET_TAXASSESSOR_0002_012.zip?download=true https://file.ac/api/3/link/7JuNY3lXM5U/READYDOTNET_TAXASSESSOR_0002_013.zip?download=true https://file.ac/api/3/link/UIq4NFNpVrQ/READYDOTNET_TAXASSESSOR_0002_014.zip?download=true https://file.ac/api/3/link/1mZgxo82_BA/READYDOTNET_TAXASSESSOR_0002_015.zip?download=true https://file.ac/api/3/link/JkmotVZ3hU8/READYDOTNET_TAXASSESSOR_0002_016.zip?download=true https://file.ac/api/3/link/5tHfIBGZezM/READYDOTNET_TAXASSESSOR_0003.zip?download=true >> attom-wget.log 2>&1 &


process_id=$!
wait $process_id
echo "Exit status: $?" >> attom-wget.log
echo " ========= STARTING @ ${time}  ========= "  >> attom-wget.log
echo " ========= COMPLETE @ $(date '+%F@%H:%M')  ========= " >> attom-wget.log


## ls -lhS /home/nhat/demand-points/attom/data/attom-raw/*

## THEN:
## RENAME commands
# downloaded files names have form: 
# READYDOTNET_TAXASSESSOR_0002_016.zip?download=true

# cd /home/nhat/demand-points/attom/data/attom-raw/
# THEN
# rename -v 's/\?download=true//' *

# OPTIONAL: rename -v 's/READYDOTNET_TAXASSESSOR_0002_0//' *
# OPTIONAL: mv READYDOTNET_TAXASSESSOR_0003.zip delta01.zip


# AFTERWARD UNZIP all files by: for z in *.zip; do unzip "$z"; done
# RENAME TXT files: rename -v 's/READYDOTNET_TAXASSESSOR_0002_0//' *

