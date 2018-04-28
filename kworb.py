from datetime import date, timedelta
from bs4 import BeautifulSoup
import urllib2
import csv
import json

url = "http://kworb.net/pop/archive/"
d1 = date(2013, 8, 13)  # start date
d2 = date(2013, 8, 14)  # end date
delta = d2 - d1         # timedelta

for i in range(delta.days + 1):
    
	#Calcula datas
    data = d1 + timedelta(days=i)
    data = data.strftime('%Y%m%d')
   
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
    headers[3:len(headers)+1] = headers[2:-1]
    headers[1] = splited[0]
    headers[2] = splited[1]


    # Produz arquivo .csv
    with open("arquivos/" + data + ".out.csv", "w") as f:
        wr = csv.writer(f)
        wr.writerow(headers)
        i = 1
        while i < len(table_data):
            splited = table_data[i][1].split('-')   
            table_data[i][3:len(row)+1] = table_data[i][2:-1]
            table_data[i][1] = splited[0]
            table_data[i][2] = splited[1]
            wr.writerow(table_data[i])
            i += 1
        f.close()