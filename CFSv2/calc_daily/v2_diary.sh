#!/bin/bash

SECONDS=0
#year=2012
variable=tmin_2m

#touch log_diario.txt
#cat >log_diario.txt << 'Trabajando en '$year

#echo 'Variable: '$variable


for ((year=2012;year<=2019;year++)); do
	for ((i=1;i<=12;i++)); do
		python v2_calc_daily.py $year $i $variable > ./zLOG/$year\_$i\_$variable\_calculo_diario.log &
		pid0=$!
		wait $pid0
	done
done

duration=$SECONDS
#echo "$((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

