#!/usr/bin/env python
# coding: utf-8

# 1. Cargar la data (automatico / csv)
# 1b. Carga de la data auxiliar
# 2. Mensualizar o no menzualizar
# 3. Definir las tranformaciones
# 4. Transformar
# 5. Exportar a csv / a la base

# In[1]:


import pandas as pd
import datetime
import requests
import datetime as dt_
from datetime import datetime as date_
from urllib.request import urlopen
from lxml import etree
from lxml import html
 
import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


header = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding":"gzip, deflate",
    "Accept-Language":"es-ES,es;q=0.9",
    "Cache-Control":"max-age=0",
    "Connection" : "keep-alive",
    "Content-Length" : "21",
    "Content-Type" : "application/x-www-form-urlencoded",        
    "Host":"www2.bmf.com.br",
    "Origin":"http://www2.bmf.com.br",
    "Referer": "http://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-tipo-de-participante-ptBR.asp",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
}


# In[3]:



def get_data(fecha):
    #cat =  "17/10/2005"
    #fecha = date_.strptime(cat, "%d/%m/%Y").strftime('%d/%m/%Y')
    #form = {"dData1": "17/10/2005"}
    url = "http://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-tipo-de-participante-ptBR.asp"
    form = {"dData1": fecha}
    
    response = requests.post(url, data=form, headers=header, verify=False)
    tree = html.fromstring(response.content)

    products = tree.xpath('.//table')
    #print(len(products))
    itemlist = []
    for product in products:

        item = {}
        #fecha = datetime.datetime.strptime(response.meta["CatURL"].replace("%2F", "//"), "%d/%m/%Y")
        item['Date'] = fecha
        item['especie'] = product.xpath('./caption//text()')[0].strip()
        personas = product.xpath('./tbody/tr')
        for fila_persona in personas:                
            persona = fila_persona.xpath('./td[1]/text()')

            if persona:
                item['persona'] = persona[0]
            else:
                item['persona'] = fila_persona.xpath('./td[1]/strong/text()')[0].strip().replace(".","").replace(",",".")
            item['compra'] = fila_persona.xpath('./td[2]/text()')[0].replace(".","").replace(",",".")
            item['compra%'] = fila_persona.xpath('./td[3]/text()')[0].replace(".","").replace(",",".")
            item['venta'] = fila_persona.xpath('./td[4]/text()')[0].replace(".","").replace(",",".")
            item['venta%'] = fila_persona.xpath('./td[5]/text()')[0].replace(".","").replace(",",".")
            itemlist.append(item.copy())
    return pd.DataFrame(itemlist)


# In[4]:


df_agg = pd.DataFrame()
weekdays = [5,6]
base = date_.today()
#numdays = 3000
#date_list = [base - dt_.timedelta(days=x+6000) for x in range(numdays)]

numdays = 15
date_list = [base - dt_.timedelta(days=x) for x in range(numdays)]


i = 0
for date in date_list:
    
    if date.weekday() in weekdays:      
        continue
    if i % 50 == 0:
        print(date)    
    
    i = i +1
    df_agg = df_agg.append(get_data(date.strftime("%d/%m/%Y")))
    
df_agg    


# In[ ]:





# In[5]:


df_agg.persona = df_agg.persona.str.strip()
df_agg.especie = df_agg.especie.str.strip()
for field in ["compra","venta"]:
    #df_agg[field] = pd.to_numeric(df_agg[field].str.replace(".","").str.replace(",","."))
    df_agg[field] = pd.to_numeric(df_agg[field])
df_agg["country"] = df_agg["especie"] + " - " + df_agg["persona"]
df_agg["neta"] = df_agg["compra"] - df_agg["venta"]
del df_agg["venta%"]
del df_agg["compra%"]
df_agg


# In[6]:


df_agg_wide = df_agg.drop_duplicates()[["especie", "persona", "neta", "Date"]].set_index(["especie", "persona", "Date"]).unstack(level=1).fillna(0)
df_agg_wide.columns = df_agg_wide.columns.droplevel()
df_agg_wide = pd.DataFrame(df_agg_wide.to_records())
for column in df_agg_wide.columns:
    df_agg_wide = df_agg_wide.rename(columns={column: column.replace("ã", "a")})
    df_agg_wide = df_agg_wide.rename(columns={column: column.replace("'", "")})
df_agg_wide = df_agg_wide.rename(columns={"especie": "country"})   
df_agg_wide = df_agg_wide.dropna(subset=["country"])
df_agg_wide = df_agg_wide[df_agg_wide["country"]!="nan"]
df_agg_wide["Date"] = pd.to_datetime(df_agg_wide["Date"], format ="%d/%m/%Y" )

df_agg_wide = df_agg_wide.set_index("Date")

alphacast.datasets.dataset(71).upload_data_from_df(df_agg_wide, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
