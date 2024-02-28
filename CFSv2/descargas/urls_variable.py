
from string import Template
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange


url_template = Template(f"$a_year/$a_year$a_month/$a_year$a_month$a_day/$a_year$a_month$a_day$a_hour")
file_template = Template("$variable.01.$t_year$t_month$t_day$t_hour.daily.grb2")
target_hours = ['00', '06', '12', '18']


def gen_filenames(year: int, month: int, day: int, hour: int):
    """
    Genera los nombres de los archivos relaciones al año, mes y día indicados.
    {var_name}.01.{year}{month}{day}{hour}.grb2
    
    Ejemplo para lluvia el 2011-02-25 ():
    prate.01.2011022500.daily.grb2

    :param a_year: el año para el que se quieren generar los archivos
    :param a_month: el mes para el que se quieren generar los archivos
    :param a_day: el día para el que se quieren generar los archivos
    :return: una lista con los nombres de los archivos existentes
    """
    variables = ['prate','tmax','tmin','tmp2m','q2m','pressfc', 'dlwsfc','ulwsfc','dswsfc','wnd10m']
    init_data = date(year, month, day)
    print(init_data)

    for a_variable in variables:
        a_year = str(year)
        a_month = str(month).zfill(2)
        a_day = str(day).zfill(2)
        a_hour = str(hour).zfill(2)
        file_name = file_template.substitute(
                    variable=a_variable, t_year=a_year, t_month=a_month, t_day=a_day, t_hour=a_hour)
        #file_name = variable + '.01.' + str(year) + str(month).zfill(2) + str(day).zfill(2)+str(hour).zfill(2) + '.daily.grb2'
        yield file_name


def gen_urls(a_year: int, a_month: int, a_day: int, a_hour: int, g_month: int, base_url: str):
    if (g_month == 1) & (a_month == 12):
        print('Cambie el año')
        a_year = a_year - 1
    date_url = url_template.substitute(a_year=a_year, a_month=str(a_month).zfill(2), a_day=str(a_day).zfill(2), a_hour=str(a_hour).zfill(2))
    for filename in gen_filenames(a_year, a_month, a_day, a_hour):
        yield f"{base_url}/{date_url}/{filename}"


if __name__ == "__main__":

    url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/operational-9-month-forecast/time-series/"

    with open('urls_generadas_2011-04-11.txt', 'w') as f:
        for url in gen_urls(2011, 4, 11, 0, 5, url_base):
            f.write(url + '\n')
