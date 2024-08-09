#!/bin/bash

SECONDS=0

year=2011
echo 'Trabajando en '$year
for ((i=1;i<=4;i++)); do
    echo 'Trabajando en mes '$i
    python v1_recorte_SISSA.py $year $i 0 > ./zTXT/$year\_$i\_0.txt &
    pid0=$!
    python v1_recorte_SISSA.py $year $i 1 > ./zTXT/$year\_$i\_1.txt &
    pid1=$!
    python v1_recorte_SISSA.py $year $i 2 > ./zTXT/$year\_$i\_2.txt &
    pid2=$!
    python v1_recorte_SISSA.py $year $i 3 > ./zTXT/$year\_$i\_3.txt &
    pid3=$!
    python v1_recorte_SISSA.py $year $i 4 > ./zTXT/$year\_$i\_4.txt &
    pid4=$!
    python v1_recorte_SISSA.py $year $i 5 > ./zTXT/$year\_$i\_5.txt &
    pid5=$!
    wait $pid0 $pid1 $pid2 $pid3 $pid4 $pid5

done

duration=$SECONDS
echo "$((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

