#!/bin/bash

SECONDS=0

year=2011
echo 'Trabajando en '$year
for ((i=7;i<=12;i++)); do
    echo 'Trabajando en mes '$i
    python v2_recorte_SISSA.py $year $i 0 > ./zTXT/$year\_$i\_0_variables.txt &
    pid0=$!
    python v2_recorte_SISSA.py $year $i 4 > ./zTXT/$year\_$i\_1_variables.txt &
    pid1=$!
    python v2_recorte_SISSA.py $year $i 8 > ./zTXT/$year\_$i\_2_variables.txt &
    pid2=$!
    python v2_recorte_SISSA.py $year $i 12 > ./zTXT/$year\_$i\_3_variables.txt &
    pid3=$!
    python v2_recorte_SISSA.py $year $i 16 > ./zTXT/$year\_$i\_4_variables.txt &
    pid4=$!
    python v2_recorte_SISSA.py $year $i 20 > ./zTXT/$year\_$i\_5_variables.txt &
    pid5=$!
    wait $pid0 $pid1 $pid2 $pid3 $pid4 $pid5

done

duration=$SECONDS
echo "$((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

