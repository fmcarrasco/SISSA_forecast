#!/bin/bash

SECONDS=0
year=2014
variable=tmin_2m

echo 'Trabajando en '$year
echo 'Variable: '$variable

for ((i=1;i<=12;i++)); do
    echo 'Trabajando en mes '$i
    python v2_calc_daily.py $year $i $variable > ./zLOG/$year\_$i\_$variable\_calculo_diario.log &
    pid0=$!
    wait $pid0

done

duration=$SECONDS
echo "$((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

