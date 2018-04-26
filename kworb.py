from datetime import date, timedelta
from bs4 import BeautifulSoup
import urllib2
import csv

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

    # #Arquivo de teste
    # test = open("tabela.txt","w")
    # test.write(table)
    # test.close()

    headers = [th.text.encode("utf-8") for th in table.select("tr th")]
    
    #Produz arquivo .csv
    with open("arquivos/" + data + ".out.csv", "w") as f:
        wr = csv.writer(f)
        wr.writerow(headers)
        wr.writerows([[td.text.encode("utf-8") for td in row.find_all("td")] for row in table.select("tr + tr")])
        f.close()