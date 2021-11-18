#!/usr/bin/env python
# coding: utf-8

# In[26]:


import pandas as pd
from datetime import date
import datetime
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[27]:


monedas = {"1": "AFEGANE AFEGANIST", "2331": "ARIARY MADAGASCAR", "4": "AUSTRAL", "6": "BALBOA/PANAMA", "5": "BATH/TAILANDIA", "2": "selected=", "3": "BIRR/ETIOPIA", "2472": "BOLIVAR SOBERANO VENEZUELANO", "8": "BOLIVAR VENEZUELANO", "7": "BOLIVAR/VENZUELA", "9": "BOLIVIANO/BOLIVIA", "234": "BUA", "10": "CEDI GANA", "11": "COLON/COSTA RICA", "12": "COLON/EL SALVADOR", "14": "CORDOBA OURO", "13": "CORDOBA/NICARAGUA", "15": "COROA DINAMARQUESA", "17": "COROA ESLOVACA", "16": "COROA ESTONIA", "18": "COROA ISLND/ISLAN", "19": "COROA NORUEGUESA", "20": "COROA SUECA", "21": "COROA TCHECA", "23": "CRUZADO", "22": "CRUZADO NOVO", "24": "CRUZEIRO", "25": "CRUZEIRO", "26": "CRUZEIRO REAL", "27": "CUPON GEORGIANO", "28": "DALASI/GAMBIA", "29": "DINAR ARGELINO", "32": "DINAR IEMENITA", "34": "DINAR IUGOSLAVO", "38": "DINAR SERVIO SERVIA", "31": "DINAR/BAHREIN", "33": "DINAR/IRAQUE", "35": "DINAR/JORDANIA", "30": "DINAR/KWAIT", "36": "DINAR/LIBIA", "37": "DINAR/MACEDONIA", "40": "DINAR/TUNISIA", "41": "DIREITO ESPECIAL", "43": "DIRHAM/EMIR.ARABE", "42": "DIRHAM/MARROCOS", "44": "DOBRA S TOME PRIN", "2492": "DOBRA SÃO TOMÉ E PRÍNCIPE", "45": "DOLAR AUSTRALIANO", "53": "DOLAR BRUNEI", "48": "DOLAR CANADENSE", "59": "DOLAR CARIBE ORIENTAL", "54": "DOLAR CAYMAN", "55": "DOLAR CINGAPURA", "49": "DOLAR DA GUIANA", "50": "DOLAR DA NAMIBIA", "67": "DOLAR DAS ILHAS SALOMAO", "68": "DOLAR DO SURINAME", "61": "DOLAR DOS EUA", "56": "DOLAR FIJI", "57": "DOLAR HONG KONG", "64": "DOLAR LIBERIA", "65": "DOLAR MALAIO", "66": "DOLAR NOVA ZELANDIA", "60": "DOLAR ZIMBABUE", "224": "DOLAR-BULGARIA", "223": "DOLAR-EX-ALEM.ORI", "225": "DOLAR-GRECIA", "226": "DOLAR-HUNGRIA", "227": "DOLAR-ISRAEL", "228": "DOLAR-IUGOSLAVIA", "229": "DOLAR-POLONIA", "231": "DOLAR-ROMENIA", "46": "DOLAR/BAHAMAS", "51": "DOLAR/BARBADOS", "52": "DOLAR/BELIZE", "47": "DOLAR/BERMUDAS", "62": "DOLAR/ETIOPIA", "63": "DOLAR/JAMAICA", "78": "DOLAR/SURINAME", "58": "DOLAR/TRIN. TOBAG", "69": "DONGUE/VIETNAN", "70": "DRACMA/GRECIA", "71": "DRAM ARMENIA REP", "72": "ESCUDO CABO VERDE", "73": "ESCUDO PORTUGUES", "74": "ESCUDO TIMOR LESTE", "222": "EURO", "79": "FLORIM HOLANDES", "76": "FLORIM/ARUBA", "77": "FLORIM/SURINAME", "80": "FORINT/HUNGRIA", "82": "FRANCO BELGA FINA", "81": "FRANCO BELGA/BELG", "87": "FRANCO CFA BCEAO", "86": "FRANCO CFA BEAC", "88": "FRANCO CFP", "85": "FRANCO COMORES", "83": "FRANCO CONGOLES", "91": "FRANCO FRANCES", "93": "FRANCO LUXEMBURGO", "94": "FRANCO MALGAXE MADAGA", "95": "FRANCO MALI", "97": "FRANCO SUICO", "84": "FRANCO/BURUNDI", "89": "FRANCO/BURUNDI", "90": "FRANCO/DJIBUTI", "92": "FRANCO/GUINE", "96": "FRANCO/RUANDA", "235": "FUA", "98": "GOURDE HAITI", "99": "GUARANI/PARAGUAI", "75": "GUILDER ANTILHAS HOLANDESAS", "100": "HRYVNIA UCRANIA", "101": "IENE", "102": "INTI PERUANO", "172": "KARBOVANETS", "173": "KINA DE PAPUA NOVA GUINE", "174": "KUNA DA CROACIA", "139": "KWANZA/ANGOLA", "103": "LARI GEORGIA", "104": "LAT LETONIA REP", "105": "LEK ALBANIA REP", "106": "LEMPIRA/HONDURAS", "107": "LEONE/SERRA LEOA", "108": "LEU/MOLDAVIA", "109": "LEU/ROMENIA", "111": "LEV/BULGARIA", "112": "LIBRA CIP/CHIPRE", "113": "LIBRA DE GIBRALTAR", "121": "LIBRA DE SANTA HELENA", "115": "LIBRA ESTERLINA", "118": "LIBRA ISRAELENSE", "123": "LIBRA SUDANESA", "2372": "LIBRA SUL SUDANESA", "114": "LIBRA/EGITO", "116": "LIBRA/FALKLAND", "117": "LIBRA/IRLANDA", "119": "LIBRA/LIBANO", "122": "LIBRA/SIRIA", "124": "LILANGENI/SUAZIL", "125": "LIRA ITALIANA", "142": "LIRA TURCA", "126": "LIRA TURQUIA", "120": "LIRA/MALTA", "127": "LITA LITUANIA", "128": "LOTI/LESOTO", "130": "MANAT ARZEBAIJAO", "129": "MARCO", "132": "MARCO ALEMAO", "133": "MARCO CONV BOSNIA", "134": "MARCO FINLANDES", "135": "METICAL MOCAMBIQ", "138": "NAIRA NIGERIA", "137": "NAKFA DA ERITREIA", "149": "NGULTRUM/BUTAO", "39": "NOVA LIBRA SUDANESA", "136": "NOVA METICAL/MOCA", "140": "NOVO DINAR IUGOSLAVO", "141": "NOVO DOLAR/TAIWAN", "110": "NOVO LEU DA ROMENIA", "131": "NOVO MANAT TURCOMANO", "145": "NOVO PESO URUGUAI", "146": "NOVO PESO URUGUAI", "143": "NOVO PESO/MEXICO", "144": "NOVO PESO/MEXICO", "147": "NOVO SOL/PERU", "148": "NOVO ZAIRE", "220": "NOVO ZAIRE", "219": "NOVO ZAIRE  ZAIRE", "236": "OURO", "151": "PAANGA/TONGA", "232": "PALADIO", "152": "PATACA/MACAU", "154": "PESETA ESPANHOLA", "153": "PESETA/ANDORA", "155": "PESO ARGENTINO", "156": "PESO ARGENTINO", "157": "PESO BOLIVIANO", "158": "PESO CHILE", "163": "PESO GUINE BISSAU", "164": "PESO MEXICANO", "159": "PESO/COLOMBIA", "160": "PESO/CUBA", "162": "PESO/FILIPINAS", "165": "PESO/MEXICO", "161": "PESO/REP. DOMINIC", "166": "PESO/URUGUAIO", "233": "PLATINA", "230": "PRATA", "167": "PULA/BOTSWANA", "169": "QUACHA ZAMBIA", "2352": "QUACHA ZAMBIA", "168": "QUACHA/MALAVI", "170": "QUETZAL/GUATEMALA", "171": "QUIATE/BIRMANIA", "175": "QUIPE/LAOS", "176": "RANDE/AFRICA SUL", "177": "REAL BRASIL", "178": "RENMINBI CHINES", "2332": "RENMINBI HONG KONG", "183": "RIAL/ARAB SAUDITA", "179": "RIAL/CATAR", "181": "RIAL/IEMEN", "182": "RIAL/IRAN", "180": "RIAL/OMA", "184": "RIEL/CAMBOJA", "185": "RINGGIT/MALASIA", "186": "RUBLO BELARUS", "2432": "RUBLO BELARUS", "187": "RUBLO/RUSSIA", "195": "RUFIA/MALDIVAS", "191": "RUPIA SEYCHELES", "193": "RUPIA/INDIA", "194": "RUPIA/INDONESIA", "189": "RUPIA/MAURICIO", "190": "RUPIA/NEPAL", "196": "RUPIA/PAQUISTAO", "192": "RUPIA/SRI LANKA", "197": "SHEKEL/ISRAEL", "198": "SOL PERUANO", "199": "SOM DO QUIRGUISTAO", "200": "SOM UZBEQUISTAO", "188": "SOMONI TADJIQUISTAO", "201": "SUCRE EQUADOR", "202": "TACA/BANGLADESH", "203": "TALA", "204": "TALA DA SAMOA OCIDENTAL", "2412": "TALA SAMOA", "205": "TENGE CAZAQUISTAO", "206": "TOLAR/ESLOVENIA", "207": "TUGRIK/MONGOLIA", "150": "UGUIA MAURITANIA", "2452": "UGUIA MAURITANIA", "208": "UNID FOMENTO CHIL", "209": "UNID.MONET.EUROP.", "2392": "UNIDADE DE FOMENTO DO CHILE", "2512": "UNIDADE DE VALOR REAL", "210": "VATU VANUATU", "212": "WON COREIA SUL", "211": "WON/COREIA NORTE", "213": "XELIM AUSTRIACO", "214": "XELIM DA TANZANIA", "215": "XELIM DA TANZANIA", "216": "XELIM/QUENIA", "218": "XELIM/SOMALIA", "217": "XELIM/UGANDA", "221": "ZLOTY/POLONIA", }


# In[36]:


df_append = pd.DataFrame()

today = date.today().strftime('%d/%m/%Y')
yesterday = (date.today() - datetime.timedelta(days=15)).strftime('%d/%m/%Y')
for moneda in monedas.keys():    
    url = "https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda={}&DATAINI={}&DATAFIM={}"
    try:
        print(url.format(moneda, yesterday, today))
        df = pd.read_csv(url.format(moneda, yesterday, today), delimiter=";", header=None)
        df["Moeda"] = monedas[moneda]
        df_append = df_append.append(df)
        print("{}: {}".format(moneda, df.shape))
    except:        
        print("{}: Failed".format(moneda))
        continue
    


# df_orig = df_append

# In[37]:


df_append = df_append.rename(columns={
    0: "Date_orig",
    2: "Type",
    3: "Currency Code",
    4: "Bid - vs BRL",
    5: "Ask - vs BRL",
    6: "Bid - vs USD",
    7: "Ask - vs USD",
})

df_append["Currency"] = df_append["Moeda"] + " (" + df_append["Currency Code"] + " " + df_append["Type"] + ")" 
df_append["Date"] = pd.to_datetime(("0" + df_append["Date_orig"].astype("str")).str[-8:],format='%d%m%Y')


# In[38]:


del df_append[1]
del df_append["Date_orig"]


# In[39]:


df_append = df_append.drop_duplicates(subset=["Date", "Currency"], keep="first")


# In[40]:


#alphacast.datasets.create("Financial - Brazil - BCB - Daily BRL exchange rates", 374, "Banco Central do Brazil daily exchange rates", )


# In[19]:


alphacast.datasets.dataset(7982).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Currency"], dateFormat= "%Y-%m-%d")


# import time
# for i in range(0,15):
#     print(i)
#     a = alphacast.datasets.dataset(7982).upload_data_from_df(df_append[i*100000:(i+1)*100000], deleteMissingFromDB = False, onConflictUpdateDB = False, uploadIndex=False)        
#     print(a)
#     time.sleep(60)
#     

# In[41]:


alphacast.datasets.dataset(7982).upload_data_from_df(df_append, deleteMissingFromDB = False, onConflictUpdateDB = False, uploadIndex=False)


# In[42]:


#alphacast.datasets.dataset(7982).processes()


# In[ ]:




