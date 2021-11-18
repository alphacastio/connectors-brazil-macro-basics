#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd

import requests 
from requests.auth import HTTPBasicAuth 
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import ast
import math
import datetime as dt
import numpy as np
from dateutil.relativedelta import relativedelta
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[24]:


def get_all_repos():
    url = "https://api.alphacast.io/repositories"
    r = requests.get(url, auth=HTTPBasicAuth(API_KEY, ""))
    return r.content


# In[25]:


def get_datasets(repo_id):
    url = "https://api.alphacast.io/datasets"
    form={
        "repositoryId": repo_id
    }
    r = requests.get(url, data=form, auth=HTTPBasicAuth(API_KEY, ""))
    return r.content


# In[26]:


def get_dataset_by_name(dataset_name, repo_id):
#creo el dataset en el repositorio si no existe
    url = "https://api.alphacast.io/datasets"
    
    r = requests.get(url, auth=HTTPBasicAuth(API_KEY, ""))
    dataset = None
    for element in json.loads(r.content):
        if (element["name"] == dataset_name) & (element["repositoryId"] == repo_id):
            dataset = element
    return dataset


# In[27]:


def create_dataset(dataset_name, repo_id):
    url = "https://api.alphacast.io/datasets"
    form={
        "name": dataset_name, 
        "repositoryId": repo_id
    }
    dataset = requests.post(url, data=form, auth=HTTPBasicAuth(API_KEY, ""))
    dataset = json.loads(dataset.content)
    return dataset


# In[28]:


def get_or_create_dataset(dataset_name, repo_id):
    dataset = get_dataset_by_name(dataset_name, repo_id)
    if not dataset:
        dataset = create_dataset(dataset_name, repo_id)
        print("Dataset Created: {}".format(dataset["id"]))
    else:
        print("Dataset Already existed: {}".format(dataset["id"]))
    return dataset


# In[29]:


def upload_data(df, dataset_id):
    url = "https://api.alphacast.io/datasets/{}/data?deleteMissingFromDB=False&onConflictUpdateDB=True".format(dataset_id)
    files = {'data': df.to_csv()}
    r = requests.put(url, files=files, auth=HTTPBasicAuth(API_KEY, ""))
    return r.content


urls = {'Anual':'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10000000&$format=json&$select=Indicador,IndicadorDetalhe,Data,DataReferencia,Media,Mediana,DesvioPadrao',
        'Trimestral':'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTrimestrais?$top=100000000&$format=json&$select=Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao',
        'Mensal':'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativaMercadoMensais?$top=10000000&$format=json&$select=Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao'}

header= {"authorization" : "basic auth"}
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('..//Sources//client_secret.json', scope)
client = gspread.authorize(creds)

df_definitions = pd.DataFrame(client.open("sgs datasets").worksheet('Expectativas').get_all_records())## Cambiar fuente del spreadsheets

df_definitions['IndicadorDetalhe'] = df_definitions['IndicadorDetalhe'].replace(np.nan, '', regex=True)
df_definitions['VarCode'] = df_definitions['Indicador'] + df_definitions['IndicadorDetalhe']
df_definitions

dataset_def = {}
for idConector in df_definitions["dataset_name"].unique():
    print(idConector)
    dataset_single = {}
    dataset_single["repo"] = df_definitions[df_definitions["dataset_name"]==idConector]["Repo"].unique()[0]
    dataset_single["repo"] = "Alphacast Brazil Private Repo"
    dataset_single["dataset"] = df_definitions[df_definitions["dataset_name"]==idConector]["dataset_name"].unique()[0]          
    dataset_single["SistemadeExpectativas"] = df_definitions[df_definitions["dataset_name"]==idConector]["SistemadeExpectativas"].unique()[0]          
    dataset_single["PeriodosaFrente"] = df_definitions[df_definitions["dataset_name"]==idConector]["PeriodosaFrente"].unique()[0]  
    dataset_single["Calculo"] = df_definitions[df_definitions["dataset_name"]==idConector]["Calculo"].unique()[0]          
    dataset_single["variables"] = {}
    
    for i, variable in df_definitions[df_definitions["dataset_name"]==idConector].iterrows():
        dataset_single["variables"][variable["VarCode"]] = [variable["VarName"]]        
    dataset_def[idConector] = dataset_single
    print(str(idConector)+ " stored")
    
dataset_def    

#Loading the the data
df_anual = pd.read_json(urls['Anual'])
df_anual = pd.DataFrame.from_dict(dict(df_anual['value']),orient = 'index')

df_trimestral = pd.read_json(urls['Trimestral'])
df_trimestral = pd.DataFrame.from_dict(dict(df_trimestral['value']),orient = 'index')

df_mensal = pd.read_json(urls['Mensal'])
df_mensal = pd.DataFrame.from_dict(dict(df_mensal['value']),orient = 'index')


def load_data(dataset,df_anual,df_trimestral,df_mensal, get_names = True):
    
    
   if dataset['SistemadeExpectativas'] == 'Anual':   
       
       df_anual['CodeVar'] = df_anual['Indicador'].astype(str) + df_anual['IndicadorDetalhe'].astype(str)
       df_anual['DataAno'] = df_anual['Data'].astype(str).str[0:4]
       df_anual['DataAno'] = df_anual['DataAno'].astype(int) + dataset['PeriodosaFrente']
       df_anual['DataAno'] = df_anual['DataAno'].astype(str)
        #selecting only monday times
       df_anual['Data'] = pd.to_datetime(df_anual['Data'])
       df_anual = df_anual[df_anual['Data'].dt.dayofweek==0]
    #Selecting the period ahead
       df_anual_nahead =df_anual[df_anual['DataAno'].astype(str)==df_anual['DataReferencia'].astype(str)]
       df_append = pd.DataFrame()
       for codename in dataset["variables"]:
           
           print(codename)
           df_single = df_anual_nahead[df_anual_nahead['CodeVar'] == codename]
           df_single = df_single.sort_values(by = 'Data')
           df_single = df_single.set_index(df_single['Data'])
           df_single = df_single.drop_duplicates(subset=['Data'], keep='first')
           df_single = df_single[dataset['Calculo']]
           df_single = df_single.rename(dataset["variables"][codename][0])
           df_append = pd.merge(df_append,df_single,how = "outer",left_index=True, right_index=True)
   if   dataset['SistemadeExpectativas'] == 'Trimestral':
        df_trimestral['Data'] = pd.to_datetime(df_trimestral['Data'])
        df_trimestral = df_trimestral[df_trimestral['Data'].dt.dayofweek==0]
        df_trimestral_nahead = df_trimestral
        df_trimestral_nahead['QANO'] = df_trimestral_nahead['Data'] + dt.timedelta(weeks = int(14*dataset['PeriodosaFrente']))
        df_trimestral_nahead['Quarter'] = df_trimestral_nahead['QANO'].dt.quarter
        df_trimestral_nahead['Year'] = df_trimestral_nahead['QANO'].dt.year
        df_trimestral_nahead['QY'] = df_trimestral_nahead['Quarter'].astype(str) + "/" + df_trimestral_nahead['Year'].astype(str)
        df_trimestral_nahead = df_trimestral_nahead[df_trimestral_nahead['QY'] == df_trimestral_nahead['DataReferencia']]
        
        df_append = pd.DataFrame()
        for codename in dataset["variables"]:
           
           print(codename)
           df_single = df_trimestral_nahead[df_trimestral_nahead['Indicador'] == codename]
           df_single = df_single.sort_values(by = 'Data')
           df_single = df_single.set_index(df_single['Data'])
           df_single = df_single.drop_duplicates(subset=['Data'], keep='first')
           df_single = df_single[dataset['Calculo']]
           df_single = df_single.rename(dataset["variables"][codename][0])
           df_append = pd.merge(df_append,df_single,how = "outer",left_index=True, right_index=True)
           
   if dataset['SistemadeExpectativas'] == 'Mensal':
       df_mensal['Data'] = pd.to_datetime(df_mensal['Data'])
       df_mensal = df_mensal[df_mensal['Data'].dt.dayofweek==0]
       df_mensal_nahead = df_mensal
       df_mensal_nahead['MANO'] = df_mensal_nahead['Data'] + dt.timedelta(days = int(30*dataset['PeriodosaFrente']))
       df_mensal_nahead['Month'] = df_mensal_nahead['MANO'].dt.month.map("{:02}".format)
       df_mensal_nahead['Year'] = df_mensal_nahead['MANO'].dt.year
       df_mensal_nahead['MY'] = df_mensal_nahead['Month'].astype(str) + "/" + df_mensal_nahead['Year'].astype(str)
       df_mensal_nahead = df_mensal_nahead[df_mensal_nahead['MY'] == df_mensal_nahead['DataReferencia']]
       df_append = pd.DataFrame()
       for codename in dataset["variables"]:
           
           print(codename)
           df_single = df_mensal_nahead[df_mensal_nahead['Indicador'] == codename]
           df_single = df_single.sort_values(by = 'Data')
           df_single = df_single.set_index(df_single['Data'])
           df_single = df_single.drop_duplicates(subset=['Data'], keep='first')
           df_single = df_single[dataset['Calculo']]
           df_single = df_single.rename(dataset["variables"][codename][0])
           df_append = pd.merge(df_append,df_single,how = "outer",left_index=True, right_index=True)

   return df_append


#Repo_id 360 is "Alphacast Brazil Private Repo"
repo_id = 360

#this are all your repos
print(json.dumps(json.loads(get_all_repos()), sort_keys=True, indent=4))


# In[35]:


for id_conector in dataset_def:
    print(id_conector)
    dataset = dataset_def[id_conector]
    df = load_data(dataset,df_anual,df_trimestral,df_mensal)
    df.index.names = ['Date']
    df["country"] = "Brazil"
    dataset_id = get_or_create_dataset(dataset["dataset"], repo_id)["id"]
    print(df)
    upload_data(df, dataset_id)      
    print("{}".format(dataset_id))    


# In[ ]:


header= {"authorization" : "basic auth"}
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('..//Sources//client_secret.json', scope)
client = gspread.authorize(creds)

df_definitions = pd.DataFrame(client.open("Transformaciones de las variables").worksheet('Conectores').get_all_records())
df_definitions = df_definitions[df_definitions["API_source"]=="Alphacast Brazil"]

for i, dataset in df_definitions.iterrows():
    print(dataset["Dataset"])
    url = "https://charts.alphacast.io/api/datasets/{}.csv".format(dataset["datasetId"])
    r = requests.get(url, auth=HTTPBasicAuth(API_KEY, "")) 
    df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

    df = pd.DataFrame(df)
    df["Date"] = pd.to_datetime(df["Year"], errors= "coerce", format= "%Y-%m-%d")    
    df = df.set_index("Date")   
    df = df.rename(columns={"Entity": "country"})
    del df["Year"]    
    
    alphacast.datasets.dataset(dataset["idConector"]).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
