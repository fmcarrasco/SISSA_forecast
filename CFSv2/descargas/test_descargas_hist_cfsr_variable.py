# 1. Import the requests library
import requests
import tqdm

#URL1 = 


#URL = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/operational-9-month-forecast/time-series/2011/201112/20111212/2011121200/prate.01.2011121200.daily.grb2"
URL = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs/2006/200612/20061212/flxf2007020212.01.2006121200.grb2"
# 2. download the data behind the URL
response = requests.get(URL)
total = int(response.headers.get('content-length', 0))
# 3. Open the response into a new file called instagram.ico
# open("acpcp_sfc_2000010500_c00.grib2", "wb").write(response.content)
#fname = "prate.01.2011121200.daily.grb2"
fname = "flxff2007020212.01.2006121200.grb2"
with open(fname, 'wb') as file, tqdm(desc=fname, total=total, unit='iB', unit_scale=True, unit_divisor=1024) as bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
