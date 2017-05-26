import requests
from bs4 import BeautifulSoup
import csv
import datetime

start_year = 2011
end_year = 2017
filename = 'wunderground_{0}-{1}.csv'.format(start_year, end_year)
months_with_30days = [4, 6, 9, 11]
column_names = ['Time(EDT)', 'Temp.', 'Dew Point', 'Humidity', 'Pressure', 'Visibility', 'Wind Dir', 'Wind Speed',
                'Gust Speed', 'Precip', 'Events', 'Conditions']
header = ['TimeEDT', 'TemperatureF', 'Dew PointF', 'Humidity', 'Sea Level PressureIn', 'VisibilityMPH',
          'Wind Direction', 'Wind SpeedMPH', 'Gust SpeedMPH', 'PrecipitationIn', 'Events', 'Conditions',
          'WindDirDegrees', 'DateUTC']


def scrap_daily_wunder_html():
    # writer = csv.DictWriter(csvfile, fieldnames=column_names)
    # writer.writeheader()

    url = 'https://www.wunderground.com/history/airport/KNYC/2017/4/3/DailyHistory.html'
    r = requests.get(url)
    # soup = BeautifulSoup(r.content, 'html.parser')
    soup = BeautifulSoup(r.content.decode('utf-8', 'ignore'), 'html.parser')
    table = soup.find('table', id="obsTable")
    rows = table.find_all('tr')[1:]
    for row in rows:
        columns = row.find_all('td')
        col_dict = {}
        for col_index, column in enumerate(columns):
            col_dict[column_names[col_index]] = str(column.getText().strip()).replace('\xa0', ' ')
        # print(col_dict)
        writer.writerow(col_dict)


def scrap_daily_wunder_csv(year, month, day):
    # writer = csv.writer(csvfile)
    url = 'https://www.wunderground.com/history/airport/KNYC/{0}/{1}/{2}/DailyHistory.html?format=1'\
                                                                                    .format(year, month, day)
    r = requests.get(url)
    contents = str(r.content).split("<br />\\n")
    # header = contents[0][4:].split(',')
    # writer.writerow(header)

    if contents[1] == 'No daily or hourly history data available':
        print(' *ERROR* : No daily or hourly history data available'.format(year, month, day))
        return

    rows = [list(row.split(',')) for row in contents[1:]][:-1]
    for row in rows:
        # print(row)
        writer.writerow(row)


def scrap_year(year):
    # check if leap year
    leap_day = 29 if year % 4 == 0 else 28
    for month in range(1, 13):
        for day in range(1, 32):
            # take care of feb
            if month == 2 and day > leap_day:
                break
            # take care of months with 30 days
            if month in months_with_30days and day > 30:
                break

            # # skip until start date if appending to file
            # if datetime.date(year, month, day) < datetime.date(2015, 8, 31):
            #     continue

            # stop after today's date
            if datetime.date(year, month, day) > datetime.date.today():
                return
            print('\r{0} {1} {2}'.format(year, month, day), end='')
            scrap_daily_wunder_csv(year, month, day)



with open(filename , 'w', newline='') as csvfile:
    # write header
    writer = csv.writer(csvfile)
    writer.writerow(header)

    for year in range(start_year, end_year+1):
        scrap_year(year)

    print("\nfinished!")

# # append to file with start year
# with open(filename , 'a', newline='') as csvfile:
#     # write header
#     writer = csv.writer(csvfile)
#     # writer.writerow(header)
#
#     for year in range(start_year, end_year+1):
#         if year < 2015:
#             continue
#         scrap_year(year)
#
#     print("\nfinished!")
