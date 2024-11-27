#!/bin/bash

base='/shera/datos/SISSA/CFSv2/'
#vari='dswrf_sfc'

declare -a arr0=("dswrf_sfc" "prate" "pres_sfc" "spfh_2m" "tmax_2m" "tmin_2m" "tmp_2m" "ugrd_hgt" "ulwrf_sfc" "vgrd_hgt" "dlwrf_sfc")
declare -a arr1=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")
YR='2019'
## now loop through the above array
for vari in "${arr0[@]}"
do
	for mes in "${arr1[@]}"
	do
		echo "Borrando "$base$vari/$YR/$mes
		rm -rf $base$vari/$YR/$mes
	done
done
