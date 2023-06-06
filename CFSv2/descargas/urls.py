
from string import Template
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange


url_template = Template(f"$a_year/$a_year$a_month/$a_year$a_month$a_day")
file_template = Template("flxf$t_year$t_month$t_day$t_hour.01.$ic_year$ic_month$ic_day$ic_hour.grb2")
target_hours = ['00', '06', '12', '18']


def gen_filenames(a_year: int, a_month: int, a_day: int, g_month: int):
    """
    Genera los nombres de los archivos relaciones al año, mes y día indicados.

    Ejemplo para el 2001-02-25 (para el primer día):
    flxf2001022500.01.2001022500.grb2
    flxf2001022506.01.2001022500.grb2 flxf2001022506.01.2001022506.grb2
    flxf2001022512.01.2001022500.grb2 flxf2001022512.01.2001022506.grb2 flxf2001022512.01.2001022512.grb2
    flxf2001022518.01.2001022500.grb2 flxf2001022518.01.2001022506.grb2 flxf2001022518.01.2001022512.grb2 flxf2001022518.01.2001022518.grb2

    Ejemplo para el 2001-02-25 (para los días siguientes):
    flxf2001022600.01.2001022500.grb2 flxf2001022600.01.2001022506.grb2 flxf2001022600.01.2001022512.grb2 flxf2001022600.01.2001022518.grb2
    flxf2001022606.01.2001022500.grb2 flxf2001022606.01.2001022506.grb2 flxf2001022606.01.2001022512.grb2 flxf2001022606.01.2001022518.grb2
    flxf2001022612.01.2001022500.grb2 flxf2001022612.01.2001022506.grb2 flxf2001022612.01.2001022512.grb2 flxf2001022612.01.2001022518.grb2
    flxf2001022618.01.2001022500.grb2 flxf2001022618.01.2001022506.grb2 flxf2001022618.01.2001022512.grb2 flxf2001022618.01.2001022518.grb2

    :param a_year: el año para el que se quieren generar los archivos
    :param a_month: el mes para el que se quieren generar los archivos
    :param a_day: el día para el que se quieren generar los archivos
    :return: una lista con los nombres de los archivos existentes
    """
    #if (g_month == 1) & (a_month == 12):
    #    print('Cambie el año')
    #    a_year = a_year - 1
    init_data = date(a_year, a_month, a_day)
    #print(init_data)
    last_date = init_data + relativedelta(months=5)
    last_date = last_date.replace(day=monthrange(last_date.year, last_date.month)[1])
    #print(last_date)
    days_between = (last_date - init_data).days

    for d in range(days_between + 1):
        ic_year, ic_month, ic_day = a_year, str(a_month).zfill(2), str(a_day).zfill(2)
        target_date = date(a_year, a_month, a_day) + timedelta(days=d)
        t_year, t_month, t_day = target_date.year, str(target_date.month).zfill(2), str(target_date.day).zfill(2)
        for i, t_hour in enumerate(target_hours):
            ic_hours = target_hours[:i+1] if d == 0 else target_hours
            for ic_hour in ic_hours:
                file_name = file_template.substitute(
                    t_year=t_year, t_month=t_month, t_day=t_day, t_hour=t_hour,
                    ic_year=ic_year, ic_month=ic_month, ic_day=ic_day, ic_hour=ic_hour)
                yield file_name


def gen_urls(a_year: int, a_month: int, a_day: int, g_month: int, base_url: str):
    if (g_month == 1) & (a_month == 12):
        print('Cambie el año')
        a_year = a_year - 1
    date_url = url_template.substitute(a_year=a_year, a_month=str(a_month).zfill(2), a_day=str(a_day).zfill(2))
    for filename in gen_filenames(a_year, a_month, a_day, g_month):
        yield f"{base_url}/{date_url}/{filename}"


if __name__ == "__main__":

    url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs"

    with open('urls_generadas_2000-12-12.txt', 'w') as f:
        for url in gen_urls(2000, 12, 12, url_base):
            f.write(url + '\n')
