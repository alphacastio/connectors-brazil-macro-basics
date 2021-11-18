#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from bs4 import BeautifulSoup
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[ ]:


#Como va a ir cambiando el link mes a mes, conviene navegar entre los nodos y descargar el más reciente
#Parseo la pagina de PIMAgro
page_0 = requests.get('https://gvagro.fgv.br/node/808')
soup_0 = BeautifulSoup(page_0.content, 'html.parser')
nivel_0 = soup_0.find_all('li', {'class':'collapsed no-dhtml'})


# In[ ]:


#Extraigo el primer link de todos, que contiene el año más reciente y parseo
page_1 = requests.get('https://gvagro.fgv.br' + nivel_0[0].a['href'])
soup_1 = BeautifulSoup(page_1.content, 'html.parser')
nivel_1 = soup_1.find('li', {'class':'expanded no-dhtml active-trail'})
nivel_2 = nivel_1.find('li', {'class':'leaf first no-dhtml'})


# In[ ]:


#Descargo y parseo el del mes más reciente
page_2 = requests.get('https://gvagro.fgv.br' + nivel_2.a['href'])
soup_2 = BeautifulSoup(page_2.content, 'html.parser')


# In[ ]:


#Extraigo el link del excel
link_xls = []
for link in soup_2.find_all('a'):
    if link.get('href') is not None and 'PIMAgro_divulgacao' in link.get('href'):
        link_xls.append('https://gvagro.fgv.br' + link.get('href'))


# In[ ]:


#Descargo el excel
xls_file = requests.get(link_xls[0])


# In[ ]:


#Cargamos las 2 hojas, serie original y desestacionalizada
df_orig = pd.read_excel(xls_file.content, sheet_name='BaseFixa')
df_sa = pd.read_excel(xls_file.content, sheet_name='BaseFixaSaz')


# In[ ]:


#Eliminamos las columnas de año y mes
df_orig.drop(['Ano', 'Mes'], axis=1, inplace=True)
df_sa.drop(['Ano', 'Mes'], axis=1, inplace=True)


# In[ ]:


#Renombramos las columnas de fechas y las seteamos como indice
df_orig.rename(columns={'Datas':'Date'}, inplace=True)
df_orig.set_index('Date', inplace=True)
df_sa.rename(columns={'Datas':'Date'}, inplace=True)
df_sa.set_index('Date', inplace=True)


# In[ ]:


#Hacemos el merge por fecha
df = df_orig.merge(df_sa.add_suffix(' - sa_orig'), left_index=True, right_index=True)


# In[ ]:


df['country'] = 'Brazil'


# In[ ]:


# dataset_name = 'Activity - Brazil - FGV - Agroindustrial Production'

# #Para raw data de Brasil
# dataset = alphacast.datasets.create(dataset_name, 374, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[ ]:


alphacast.datasets.dataset(8451).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




