
from urllib import request
from collections import namedtuple
from pathlib import Path
from hashlib import md5


MAX_FILES = 5  # Cantidad máxima de archivos a descargar (None para descargar todos)

LINE_UP = '\033[1A'  # <-- ANSI sequence
LINE_CLEAR = '\x1b[2K'  # <-- ANSI sequence

FileData = namedtuple("FileData", ["year_g", "month_g", "year", "month", "day", "name"])


def set_file_data(file_url: str, year_g: str, month_g: str) -> FileData:
    raw_data = file_url.strip().split("/")[-4:]
    file_data = (year_g, month_g, raw_data[0], raw_data[1][-2:], raw_data[2][-2:], raw_data[3])
    return FileData(*file_data)


def set_file_folder(file_data: FileData) -> Path:
    folder = Path(f"/Shera/datos/CFSv2/{file_data.year_g}/{file_data.month_g}/{file_data.year}{file_data.month}{file_data.day}")
    #folder = Path(f"/Volumes/Almacenamiento/python_proyects/DATOS/CFSv2/{file_data.year_g}/{file_data.month_g}/{file_data.year}{file_data.month}{file_data.day}")
    #folder = Path(f"descargas_cfs/{file_data.year}/{file_data.month}/{file_data.day}")
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
    return folder


def set_file_abs_path(file_url: str, year_g: str, month_g: str) -> Path:
    file_data = set_file_data(file_url, year_g, month_g)
    file_folder = set_file_folder(file_data)
    return Path(f"{file_folder}/{file_data.name}")


def check_md5(file_abs_path: Path, file_url: str) -> bool:
    if file_abs_path.exists():
        with open(file_abs_path, 'rb') as local_file:
            local_file_md5 = md5(local_file.read()).hexdigest()
        with request.urlopen(f"{file_url.strip()}.md5") as remote_data:
            remote_file_md5 = remote_data.read().decode('utf-8').strip().split()[0]
        if local_file_md5 == remote_file_md5:
            return True
    return False


def show_progress(block_num, block_size, total_size):
    porcentaje = round(block_num * block_size / total_size * 100)
    print(f"Porcentaje descargado: {porcentaje}%", end="\r")
    print(end=LINE_CLEAR)  # https://itnext.io/overwrite-previously-printed-lines-4218a9563527


def download_file_from_url(file_abs_path: Path, file_url: str):
    request.urlretrieve(
        url=file_url.strip(),
        filename=file_abs_path.absolute().as_posix(),
        reporthook=show_progress)
    if not check_md5(file_abs_path, file_url):
        file_abs_path.unlink()


if __name__ == "__main__":

    with open("urls_generadas_2001-12-12.txt", "r") as urls_file:
        urls = urls_file.readlines()
        urls = urls[0:MAX_FILES] if MAX_FILES else urls
        for n, url in enumerate(urls):
            print(f"Procesando archivo {n+1} de {len(urls)}.")
            file_path = set_file_abs_path(url)
            if not file_path.exists() or not check_md5(file_path, url):
                download_file_from_url(file_path, url)
