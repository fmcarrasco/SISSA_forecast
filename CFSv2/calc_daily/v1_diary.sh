#!/bin/bash

SECONDS=0
year=2010
variable=tmax_2m

echo 'Trabajando en '$year
echo 'Variable: '$variable

for ((i=1;i<=12;i++)); do
    echo 'Trabajando en mes '$i
    python v1_calc_daily.py $year $i $variable > ./zLOG/$year\_$i\_$variable\_calculo_diario.log &
    #python v1_calc_daily.py $year $i $variable > ./$year\_$i\_$variable\_calculo_diario.log &
    pid0=$!
    wait $pid0

done

duration=$SECONDS
echo "$((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

