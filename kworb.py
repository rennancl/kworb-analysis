from datetime import date, timedelta
from bs4 import BeautifulSoup
import urllib2
import csv
import json

url = "http://kworb.net/pop/archive/"
d1 = date(2013, 8, 13)  # start date
d2 = date(2013, 8, 14)  # end date
delta = d2 - d1         # timedelta
fj = open("arquivos/final/history.out.json", "w")
fh = open("arquivos/final/history.out.csv", "w")
wh = csv.writer(fh)
rows2 = []
for i in range(delta.days + 1):
    
	#Calcula datas
    date = d1 + timedelta(days=i)
    data = date.strftime('%Y%m%d')
   
   	#Busca e baixa pagina
    response = urllib2.urlopen('' + url + data + '.html')
    page = response.read()

    #Parsing da pagina
    soup = BeautifulSoup(page, "html.parser")
    table = soup.select_one("table.sortable")
    table_data = [[cell.text.encode(soup.original_encoding) for cell in row("td")]
        for row in table("tr")]
    headers = [th.text.encode(soup.original_encoding) for th in table.select("tr th")]
    splited = headers[1].split(' and ')   
    headers[6:len(headers)+4] = headers[2:-1]
    headers[1] = splited[0]
    headers[2] = splited[1]
    headers[3] = 'day'
    headers[4] = 'month'
    headers[5] = 'year'
    headers[6] = 'sales'
    print data

    # Produz arquivo .csv
    with open("arquivos/" + data + ".out.csv", "w") as f:
        wr = csv.writer(f)
        wr.writerow(headers[0:7])
        i = 1
        while i < len(table_data):
            splited = table_data[i][1].split(' - ')   
            table_data[i][6:len(row)+4] = table_data[i][2:-1]
            table_data[i][1] = splited[0]
            table_data[i][2] = splited[1]
            table_data[i][3] = date.day
            table_data[i][4] = date.month
            table_data[i][5] = date.year            
            wr.writerow(table_data[i][0:7])
            i += 1
        f.close()
    
    i = 1
    while i < len(table_data):          
        wh.writerow(table_data[i][0:7])
        i += 1
    
    with open("arquivos/" + data + ".out.csv", "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        rows2.extend(list(rows))
        f.close()

    with open("arquivos/" + data + ".out.json", "w") as f:
        json.dump(rows, f)
        f.close()
    
json.dump(rows2, fj)
fh.close()
fj.close()