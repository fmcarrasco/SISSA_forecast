# 1. Import the requests library
import requests
from tqdm import tqdm

URL = "https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast/2000/2000010500/c00/Days%3A1-10/acpcp_sfc_2000010500_c00.grib2"
# 2. download the data behind the URL
response = requests.get(URL)
total = int(response.headers.get('content-length', 0))
# 3. Open the response into a new file called instagram.ico
# open("acpcp_sfc_2000010500_c00.grib2", "wb").write(response.content)
fname = "acpcp_sfc_2000010500_c00.grib2"
with open(fname, 'wb') as file, tqdm(desc=fname, total=total, unit='iB', unit_scale=True, unit_divisor=1024) as bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
