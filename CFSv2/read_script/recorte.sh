#!/bin/bash
conda run -n meteo-py python recorte_SISSA_CFS2_0.py > 0.txt &
conda run -n meteo-py python recorte_SISSA_CFS2_1.py > 1.txt &
#conda run -n meteo-py python recorte_SISSA_CFS2_2.py
#conda run -n meteo-py python recorte_SISSA_CFS2_3.py
#conda run -n meteo-py python recorte_SISSA_CFS2_4.py
#conda run -n meteo-py python recorte_SISSA_CFS2_5.py

