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
    print json.dumps(dict(table_data))
    headers = [th.text.encode(soup.original_encoding) for th in table.select("tr th")]
    
    # Produz arquivo .csv
    with open("arquivos/" + data + ".out.csv", "w") as f:
        wr = csv.writer(f)
        wr.writerow(headers)
        wr.writerows(table_data)
        f.close()