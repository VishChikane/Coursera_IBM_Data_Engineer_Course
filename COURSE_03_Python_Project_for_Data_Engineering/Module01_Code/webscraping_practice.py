import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

'''
web link = https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films

'''

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_25_In_2000s'
csv_path = '/home/project/M01_WebScraping/top_25_films_2000_to_2010.csv'
df = pd.DataFrame(columns=["Film","Year","Rotten Tomatoes' Top 100"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page,'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count<50:
        col=row.find_all('td')
        if len(col)!=0:
            year = col[2].contents[0]
            if year != 'unranked' and int(year)>=2000 and int(year) < 2011:
                data_dict = {
                            "Film": col[1].contents[0],
                            "Year":col[2].contents[0],
                            "Rotten Tomatoes' Top 100":col[3].contents[0],}
                df1 = pd.DataFrame(data_dict,index=[0])
                df = pd.concat([df,df1], ignore_index=True)
                count+=1
            else:
                continue
    else:
        break

# print(df)
df.to_csv(csv_path)
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace',index=False)
conn.close()

