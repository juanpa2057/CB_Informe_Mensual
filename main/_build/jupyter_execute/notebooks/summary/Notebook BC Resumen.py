#!/usr/bin/env python
# coding: utf-8

# <!-- # Proyecto Monitoreo Bancolombia - Informe Mensual -->
# 
# # Resumen
# 
# Octubre de 2023

# ¡Hola!, te presentamos el informe correspondiente a tus consumos del mes de octubre 2023. A continuación vas a encontrar un resumen de los consumos realizados de forma acumulada. Para esto encontrarás una serie de gráficas diseñadas para dar un vistazo a los consumos por sede. Finalmente, encontrarás un informe detallado para cada sede.

# ## Definitions
# 

# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import json

import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pyppdf.patch_pyppeteer

pio.renderers.default = "notebook"
pio.templates.default = "plotly_white"


# this enables relative path imports
import os
from dotenv import load_dotenv
load_dotenv()
_PROJECT_PATH: str = os.environ["_project_path"]
_PICKLED_DATA_FILENAME: str = os.environ["_pickled_data_filename"]

import sys
from pathlib import Path
project_path = Path(_PROJECT_PATH)
sys.path.append(str(project_path))

import config_v2 as cfg

from library_ubidots_v2 import Ubidots as ubi

from library_report_v2 import Cleaning as cln
from library_report_v2 import Graphing as grp
from library_report_v2 import Processing as pro
from library_report_v2 import Configuration as repcfg

# import warnings
# warnings.filterwarnings("ignore")
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

month_name = cfg.MONTH_NAME


# ## Preprocessing

# In[2]:


df_info = pd.read_excel(project_path / 'tools' / "AMH Sedes BC.xlsx")

info_rename = {
    'Nombre en Ubidots':'device_name',
    'ÁREA [m2]':'area',
    'TARIFA [$/kWh]':'electricity_price',
    '¿SUCURSAL AUTOMATIZADA?':'automated',
    'CAPACIDAD AA [TR]':'hvac_capacity_kw',
}
df_info = df_info.rename(columns=info_rename)
df_info = df_info.loc[:, list(info_rename.values())]
df_info['automated'] = df_info['automated'].map({'Sí':True,'NO':False})



# In[3]:


df = pd.read_pickle(project_path / 'data' / _PICKLED_DATA_FILENAME)

# Legacy code (including the library) expects these column names
# but the new Ubidots library returns more specific column names
# so renaming is necessary. TODO: rework the Report library
# so that it uses these more descriptive column names.
df = df.rename(columns={'variable_label':'variable','device_label':'device',})

df = df.sort_values(by=['variable','datetime'])
df = pro.datetime_attributes(df)

# Agrego esta linea codigo para quitar estas dos sedes que generar ruido por datos altos
df = df[~df["device_name"].isin(["BC 829 - Unicentro Cali", "BC 454 - Quinta Avenida 2"])]

df_bl, df_st = pro.split_into_baseline_and_study(df, baseline=cfg.BASELINE, study=cfg.STUDY, inclusive='left')

study_daterange = pd.Series(pd.date_range(start=cfg.STUDY[0], end=cfg.STUDY[1], freq='D'))


# In[4]:


cargas = df_st[df_st["variable"].isin(cfg.ENERGY_VAR_LABELS)]
front = df_st[df_st["variable"].isin(['front-consumo-activa'])]
front_pot = df_st[df_st["variable"].isin(['front-potencia-activa'])]
front_reactiva = df_st[df_st["variable"].isin(['consumo-energia-reactiva-total'])]
cargas_pot = df_st[df_st["variable"].isin(cfg.POWER_VAR_LABELS)]
cargas_nocturne = cargas[cargas["hour"].isin(cfg.NIGHT_HOURS)]


# In[5]:


past_months = df_bl[df_bl["variable"] == 'front-consumo-activa'].groupby(by=["variable", "device", "device_name"]).resample('1M').sum().round(2).reset_index().set_index('datetime')
past_months = pro.datetime_attributes(past_months)

past_hour = df_bl[df_bl["variable"] == 'front-consumo-activa'].groupby(by=["variable", "device", "device_name"]).resample('1h').sum().round(2).reset_index().set_index('datetime')
past_hour = pro.datetime_attributes(past_hour)

cargas_month = cargas.groupby(by=["variable", "device", "device_name"]).resample('1M').sum().round(2).reset_index().set_index('datetime')
cargas_month = pro.datetime_attributes(cargas_month)

cargas_day = cargas.groupby(by=["variable", "device", "device_name"]).resample('1D').sum().round(2).reset_index().set_index('datetime')
cargas_day = pro.datetime_attributes(cargas_day)

cargas_hour = cargas.groupby(by=["variable", "device", "device_name"]).resample('1h').sum().round(2).reset_index().set_index('datetime')
cargas_hour = pro.datetime_attributes(cargas_hour)

front_hour = front.groupby(by=["variable", "device", "device_name"]).resample('1h').sum().round(2).reset_index().set_index('datetime')
front_hour = pro.datetime_attributes(front_hour)

front_month = front.groupby(by=["variable", "device", "device_name"]).resample('1M').sum().round(2).reset_index().set_index('datetime')
front_month = pro.datetime_attributes(front_month)

front_day = front.groupby(by=["variable", "device", "device_name"]).resample('1D').sum().round(2).reset_index().set_index('datetime')
front_day = pro.datetime_attributes(front_day)

front_reactiva_hour = front_reactiva.groupby(by=["variable", "device", "device_name"]).resample('1h').sum().round(2).reset_index().set_index('datetime')
front_reactiva_hour = pro.datetime_attributes(front_reactiva_hour)

Cargas_Nocturne_day = cargas_nocturne.groupby(by=["variable", "device", "device_name"]).resample('1D').sum().round(2).reset_index().set_index('datetime')
Cargas_Nocturne_day = pro.datetime_attributes(Cargas_Nocturne_day)


# ## Resultados

# In[6]:


front_tot = front_month[["value","device_name"]].reset_index(drop=True).set_index('device_name')
front_tot["Consumo - MWh"] = front_tot["value"]/1000
front_tot.drop(["value"], axis=1, inplace=True)
front_tot.reset_index(inplace=True, drop=False)
front_tot['Consumo - MWh'] = np.abs(front_tot['Consumo - MWh'])
sizes = front_tot.sort_values(by='Consumo - MWh', ascending=False)


# In[7]:


df_info_vs_cons = pd.merge(
    front_tot,
    df_info,
    how='inner'
)

df_info_vs_cons['yearly_kwh_per_m2'] = 12* 1000 * df_info_vs_cons['Consumo - MWh'] / df_info_vs_cons['area']
intensity = df_info_vs_cons[['device_name', 'yearly_kwh_per_m2']].sort_values(by='yearly_kwh_per_m2', ascending=False)


# In[8]:


sizes = sizes.query("device_name != 'BC 197 - Guatapuri'")

fig = px.bar(
    sizes.round(2), 
    x="device_name", 
    y="Consumo - MWh", 
    labels={'Consumo - MWh':'Consumo [MWh]', 'device_name':'Sede'}, 
    title="Consumo de energía activa por sede [MWh]"
    )
fig.show()


# In[9]:


fig = px.bar(
    intensity.round(2), 
    x="device_name", 
    y="yearly_kwh_per_m2", 
    labels={'yearly_kwh_per_m2':'Intensidad', 'device_name':'Sede'}, 
    title="Intensidad del consumo por sede [kWh/m^2-año]"
)
fig.show()


# Un indicador ampliamente utilizado es el consumo anual por unidad de área.
# 
# A continuación mostramos la distribución geográfica de las intensidades de consumo para las diferentes sedes.

# In[10]:


df_devices = ubi.get_available_devices_v2('bancolombia', 'group', page_size=100)
device_labels_with_data = list(set(df['device']))
has_data = df_devices['device_label'].isin(device_labels_with_data)
ids_with_data = list(df_devices.loc[has_data, 'device_id'])
df_map = ubi.get_gps_for_multiple_device_id(ids_with_data)

df_intensity = intensity.copy()
df_intensity = df_intensity.rename(columns={'yearly_kwh_per_m2':'value'})

df_map = pd.merge(
    df_map,
    # front_month[['device_name','value']],
    df_intensity,
    how='inner'
)

df_map = df_map.dropna(how='any')


fig = go.Figure()
fig.add_trace(go.Scattergeo(
    lon = df_map["longitude"],
    lat = df_map["latitude"],
    text = df_map["device_name"],
    marker = dict(
        size = df_map["value"],
        line_width=0.5,
        sizemode = 'area'
    )))


fig.update_layout(
    margin={"r":50,"t":50,"l":50,"b":50},
    geo = go.layout.Geo(
        resolution = 50,
        scope = 'south america',
        showframe = True,
        showcoastlines = True,
        landcolor = "rgb(229, 229, 229)",
        countrycolor = "white" ,
        coastlinecolor = "white",
        projection_type = 'mercator',
        lonaxis_range= [ -65.0, -85.0 ],
        lataxis_range= [ -5.0, 13.0 ],
        projection_scale=20))


fig.update_layout(title_text="Mapa de intensidad de consumo [kWh/m^2-año]", font_size=12,width=750,height=500)
fig.show()


# De las sedes monitoreadas las que presentan mayor intensidad de consumo son Paseo de la castellana, Centro Colón, Jamundi y Ventura plaza. También podemos notar que en general entre sedes cercanas hay intensidades de consumo similares.

# In[11]:


fig = px.pie(sizes, values="Consumo - MWh", names='device_name', hover_data=['Consumo - MWh'], labels={'Consumo - MWh'})
fig.update_traces(textposition='inside', textinfo='percent', insidetextorientation='radial')

fig.update(layout_showlegend=True)
fig.update_layout(title_text="Diagrama de torta consumo de energía kWh", font_size=12,width=750,height=550)
fig.show()


# La figura anterior es simplemente otra manera de visualizar la distribución del consumo. A continuación vemos la contribución de los equipos de climatización e iluminación al consumo en cada sede y al consumo agregado entre sedes.

# In[12]:


El_Cacique = front_month[front_month["device_name"]=="BC 78 - El Cacique"]["value"]
El_Cacique_cargas = cargas_month[cargas_month["device_name"]=="BC 78 - El Cacique"]
El_Cacique_ilu = El_Cacique_cargas[El_Cacique_cargas["variable"]=="ilu-consumo-activa"]["value"]
El_Cacique_aa = El_Cacique_cargas[El_Cacique_cargas["variable"]=="aa-consumo-activa"]["value"]

Girardot = front_month[front_month["device_name"]=="BC 659 - Girardot"]["value"]
Girardot_cargas = cargas_month[cargas_month["device_name"]=="BC 659 - Girardot"]
Girardot_ilu = Girardot_cargas[Girardot_cargas["variable"]=="ilu-consumo-activa"]["value"]
Girardot_aa = Girardot_cargas[Girardot_cargas["variable"]=="aa-consumo-activa"]["value"]

Ventura_plaza = front_month[front_month["device_name"]=="BC 824 - Ventura Plaza"]["value"]
Ventura_plaza_cargas = cargas_month[cargas_month["device_name"]=="BC 824 - Ventura Plaza"]
Ventura_plaza_ilu = Ventura_plaza_cargas[Ventura_plaza_cargas["variable"]=="ilu-consumo-activa"]["value"]
Ventura_plaza_aa = Ventura_plaza_cargas[Ventura_plaza_cargas["variable"]=="aa-consumo-activa"]["value"]

Banca_Colombia_Cartagena = front_month[front_month["device_name"]=="BC 210 - Banca Colombia Cartagena"]["value"]
Banca_Colombia_Cartagena_cargas = cargas_month[cargas_month["device_name"]=="BC 210 - Banca Colombia Cartagena"]
Banca_Colombia_Cartagena_ilu = Banca_Colombia_Cartagena_cargas[Banca_Colombia_Cartagena_cargas["variable"]=="ilu-consumo-activa"]["value"]
Banca_Colombia_Cartagena_aa = Banca_Colombia_Cartagena_cargas[Banca_Colombia_Cartagena_cargas["variable"]=="aa-consumo-activa"]["value"]

Bello = front_month[front_month["device_name"]=="BC 311 - Bello"]["value"]
Bello_cargas = cargas_month[cargas_month["device_name"]=="BC 311 - Bello"]
Bello_ilu = Bello_cargas[Bello_cargas["variable"]=="ilu-consumo-activa"]["value"]
Bello_aa = Bello_cargas[Bello_cargas["variable"]=="aa-consumo-activa"]["value"]

Cúcuta = front_month[front_month["device_name"]=="BC 88 - Cúcuta"]["value"]
Cúcuta_cargas = cargas_month[cargas_month["device_name"]=="BC 88 - Cúcuta"]
Cúcuta_ilu = Cúcuta_cargas[Cúcuta_cargas["variable"]=="ilu-consumo-activa"]["value"]
Cúcuta_aa = Cúcuta_cargas[Cúcuta_cargas["variable"]=="aa-consumo-activa"]["value"]

Barrancabermeja = front_month[front_month["device_name"]=="BC 306 - Barranquabermeja"]["value"]
Barrancabermeja_cargas = cargas_month[cargas_month["device_name"]=="BC 306 - Barranquabermeja"]
Barrancabermeja_ilu = Barrancabermeja_cargas[Barrancabermeja_cargas["variable"]=="ilu-consumo-activa"]["value"]
Barrancabermeja_aa = Barrancabermeja_cargas[Barrancabermeja_cargas["variable"]=="aa-consumo-activa"]["value"]

LLano_Grande_Palmira = front_month[front_month["device_name"]=="BC 185 - Llano Grande Palmira"]["value"]
LLano_Grande_Palmira_cargas = cargas_month[cargas_month["device_name"]=="BC 185 - Llano Grande Palmira"]
LLano_Grande_Palmira_ilu = LLano_Grande_Palmira_cargas[LLano_Grande_Palmira_cargas["variable"]=="ilu-consumo-activa"]["value"]
LLano_Grande_Palmira_aa = LLano_Grande_Palmira_cargas[LLano_Grande_Palmira_cargas["variable"]=="aa-consumo-activa"]["value"]

Palmira = front_month[front_month["device_name"]=="BC 66 - Palmira"]["value"]
Palmira_cargas = cargas_month[cargas_month["device_name"]=="BC 66 - Palmira"]
Palmira_ilu = Palmira_cargas[Palmira_cargas["variable"]=="ilu-consumo-activa"]["value"]
Palmira_aa = Palmira_cargas[Palmira_cargas["variable"]=="aa-consumo-activa"]["value"]

Villa_Colombia = front_month[front_month["device_name"]=="BC 205 - Villa Colombia"]["value"]
Villa_Colombia_cargas = cargas_month[cargas_month["device_name"]=="BC 205 - Villa Colombia"]
Villa_Colombia_ilu = Villa_Colombia_cargas[Villa_Colombia_cargas["variable"]=="ilu-consumo-activa"]["value"]
Villa_Colombia_aa = Villa_Colombia_cargas[Villa_Colombia_cargas["variable"]=="aa-consumo-activa"]["value"]

Los_Patios = front_month[front_month["device_name"]=="BC 863 - Los Patios"]["value"]
Los_Patios_cargas = cargas_month[cargas_month["device_name"]=="BC 863 - Los Patios"]
Los_Patios_ilu = Los_Patios_cargas[Los_Patios_cargas["variable"]=="ilu-consumo-activa"]["value"]
Los_Patios_aa = Los_Patios_cargas[Los_Patios_cargas["variable"]=="aa-consumo-activa"]["value"]

Jamundi = front_month[front_month["device_name"]=="BC 764 - Jamundí"]["value"]
Jamundi_cargas = cargas_month[cargas_month["device_name"]=="BC 764 - Jamundí"]
Jamundi_ilu = Jamundi_cargas[Jamundi_cargas["variable"]=="ilu-consumo-activa"]["value"]
Jamundi_aa = Jamundi_cargas[Jamundi_cargas["variable"]=="aa-consumo-activa"]["value"]

Honda = front_month[front_month["device_name"]=="BC 424 - Honda"]["value"]
Honda_cargas = cargas_month[cargas_month["device_name"]=="BC 424 - Honda"]
Honda_ilu = Honda_cargas[Honda_cargas["variable"]=="ilu-consumo-activa"]["value"]
Honda_aa = Honda_cargas[Honda_cargas["variable"]=="aa-consumo-activa"]["value"]

La_America = front_month[front_month["device_name"]=="BC 613 - La America"]["value"]
La_America_cargas = cargas_month[cargas_month["device_name"]=="BC 613 - La America"]
La_America_ilu = La_America_cargas[La_America_cargas["variable"]=="ilu-consumo-activa"]["value"]
La_America_aa = La_America_cargas[La_America_cargas["variable"]=="aa-consumo-activa"]["value"]

Guatapuri = front_month[front_month["device_name"]=="BC 197 - Guatapuri"]["value"]
Guatapuri_cargas = cargas_month[cargas_month["device_name"]=="BC 197 - Guatapuri"]
Guatapuri_ilu = Guatapuri_cargas[Guatapuri_cargas["variable"]=="ilu-consumo-activa"]["value"]
Guatapuri_aa = Guatapuri_cargas[Guatapuri_cargas["variable"]=="aa-consumo-activa"]["value"]

Lebrija = front_month[front_month["device_name"]=="BC 776 - Lebrija"]["value"]
Lebrija_cargas = cargas_month[cargas_month["device_name"]=="BC 776 - Lebrija"]
Lebrija_ilu = Lebrija_cargas[Lebrija_cargas["variable"]=="ilu-consumo-activa"]["value"]
Lebrija_aa = Lebrija_cargas[Lebrija_cargas["variable"]=="aa-consumo-activa"]["value"]

Paseo_del_comercio = front_month[front_month["device_name"]=="BC 792 - Paseo del comercio"]["value"]
Paseo_del_comercio_cargas = cargas_month[cargas_month["device_name"]=="BC 792 - Paseo del comercio"]
Paseo_del_comercio_ilu = Paseo_del_comercio_cargas[Paseo_del_comercio_cargas["variable"]=="ilu-consumo-activa"]["value"]
Paseo_del_comercio_aa = Paseo_del_comercio_cargas[Paseo_del_comercio_cargas["variable"]=="aa-consumo-activa"]["value"]

Carrera_primera = front_month[front_month["device_name"]=="BC 061 - Carrera Primera"]["value"]
Carrera_primera_cargas = cargas_month[cargas_month["device_name"]=="BC 061 - Carrera Primera"]
Carrera_primera_ilu = Carrera_primera_cargas[Carrera_primera_cargas["variable"]=="ilu-consumo-activa"]["value"]
Carrera_primera_aa = Carrera_primera_cargas[Carrera_primera_cargas["variable"]=="aa-consumo-activa"]["value"]

Iwanna = front_month[front_month["device_name"]=="BC 496 - Iwanna"]["value"]
Iwanna_cargas = cargas_month[cargas_month["device_name"]=="BC 496 - Iwanna"]
Iwanna_ilu = Iwanna_cargas[Iwanna_cargas["variable"]=="ilu-consumo-activa"]["value"]
Iwanna_aa = Iwanna_cargas[Iwanna_cargas["variable"]=="aa-consumo-activa"]["value"]

Pitalito = front_month[front_month["device_name"]=="BC 453 - Pitalito"]["value"]
Pitalito_cargas = cargas_month[cargas_month["device_name"]=="BC 453 - Pitalito"]
Pitalito_ilu = Pitalito_cargas[Pitalito_cargas["variable"]=="ilu-consumo-activa"]["value"]
Pitalito_aa = Pitalito_cargas[Pitalito_cargas["variable"]=="aa-consumo-activa"]["value"]

Giron = front_month[front_month["device_name"]=="BC 796 - Girón"]["value"]
Giron_cargas = cargas_month[cargas_month["device_name"]=="BC 796 - Girón"]
Giron_ilu = Giron_cargas[Giron_cargas["variable"]=="ilu-consumo-activa"]["value"]
Giron_aa = Giron_cargas[Giron_cargas["variable"]=="aa-consumo-activa"]["value"]

Piedecuesta = front_month[front_month["device_name"]=="BC 044 - Piedecuesta"]["value"]
Piedecuesta_cargas = cargas_month[cargas_month["device_name"]=="BC 044 - Piedecuesta"]
Piedecuesta_ilu = Piedecuesta_cargas[Piedecuesta_cargas["variable"]=="ilu-consumo-activa"]["value"]
Piedecuesta_aa = Piedecuesta_cargas[Piedecuesta_cargas["variable"]=="aa-consumo-activa"]["value"]

Floridablanca = front_month[front_month["device_name"]=="BC 799 - Floridablanca"]["value"]
Floridablanca_cargas = cargas_month[cargas_month["device_name"]=="BC 799 - Floridablanca"]
Floridablanca_ilu = Floridablanca_cargas[Floridablanca_cargas["variable"]=="ilu-consumo-activa"]["value"]
Floridablanca_aa = Floridablanca_cargas[Floridablanca_cargas["variable"]=="aa-consumo-activa"]["value"]

San_Mateo = front_month[front_month["device_name"]=="BC 834 - San Mateo"]["value"]
San_Mateo_cargas = cargas_month[cargas_month["device_name"]=="BC 834 - San Mateo"]
San_Mateo_ilu = San_Mateo_cargas[San_Mateo_cargas["variable"]=="ilu-consumo-activa"]["value"]
San_Mateo_aa = San_Mateo_cargas[San_Mateo_cargas["variable"]=="aa-consumo-activa"]["value"]

Campo_Alegre = front_month[front_month["device_name"]=="BC 459 - Campo Alegre"]["value"]
Campo_Alegre_cargas = cargas_month[cargas_month["device_name"]=="BC 459 - Campo Alegre"]
Campo_Alegre_ilu = Campo_Alegre_cargas[Campo_Alegre_cargas["variable"]=="ilu-consumo-activa"]["value"]
Campo_Alegre_aa = Campo_Alegre_cargas[Campo_Alegre_cargas["variable"]=="aa-consumo-activa"]["value"]

Paseo_de_la_castellana = front_month[front_month["device_name"]=="BC 678 - Paseo de la Castellana"]["value"]
Paseo_de_la_castellana_cargas = cargas_month[cargas_month["device_name"]=="BC 678 - Paseo de la Castellana"]
Paseo_de_la_castellana_ilu = Paseo_de_la_castellana_cargas[Paseo_de_la_castellana_cargas["variable"]=="ilu-consumo-activa"]["value"]
Paseo_de_la_castellana_aa = Paseo_de_la_castellana_cargas[Paseo_de_la_castellana_cargas["variable"]=="aa-consumo-activa"]["value"]

Calima = front_month[front_month["device_name"]=="BC 741 - Calima"]["value"]
Calima_cargas = cargas_month[cargas_month["device_name"]=="BC 741 - Calima"]
Calima_ilu = Calima_cargas[Calima_cargas["variable"]=="ilu-consumo-activa"]["value"]
Calima_aa = Calima_cargas[Calima_cargas["variable"]=="aa-consumo-activa"]["value"]

El_Bosque = front_month[front_month["device_name"]=="BC 495 - El Bosque"]["value"]
El_Bosque_cargas = cargas_month[cargas_month["device_name"]=="BC 495 - El Bosque"]
El_Bosque_ilu = El_Bosque_cargas[El_Bosque_cargas["variable"]=="ilu-consumo-activa"]["value"]
El_Bosque_aa = El_Bosque_cargas[El_Bosque_cargas["variable"]=="aa-consumo-activa"]["value"]

Santa_Monica = front_month[front_month["device_name"]=="BC 749 - Santa Monica"]["value"]
Santa_Monica_cargas = cargas_month[cargas_month["device_name"]=="BC 749 - Santa Monica"]
Santa_Monica_ilu = Santa_Monica_cargas[Santa_Monica_cargas["variable"]=="ilu-consumo-activa"]["value"]
Santa_Monica_aa = Santa_Monica_cargas[Santa_Monica_cargas["variable"]=="aa-consumo-activa"]["value"]


Las_Palmas = front_month[front_month["device_name"]=="BC 291 - Las Palmas"]["value"]
Las_Palmas_cargas = cargas_month[cargas_month["device_name"]=="BC 291 - Las Palmas"]
Las_Palmas_ilu = Las_Palmas_cargas[Las_Palmas_cargas["variable"]=="ilu-consumo-activa"]["value"]
Las_Palmas_aa = Las_Palmas_cargas[Las_Palmas_cargas["variable"]=="aa-consumo-activa"]["value"]

Megamall = front_month[front_month["device_name"]=="BC 90 - Megamall"]["value"]
Megamall_cargas = cargas_month[cargas_month["device_name"]=="BC 90 - Megamall"]
Megamall_ilu = Megamall_cargas[Megamall_cargas["variable"]=="ilu-consumo-activa"]["value"]
Megamall_aa = Megamall_cargas[Megamall_cargas["variable"]=="aa-consumo-activa"]["value"]


# In[13]:


fig = go.Figure(data=[go.Sankey(
    node = {'pad': 15,
            'thickness': 15,
            'line': {'color': 'black', 'width': 0.5},
            'label': ['Consumo total (kWh)',
                      'BC 78 - El Cacique',               
                      'BC 659 - Girardot',                
                      'BC 824 - Ventura Plaza',           
                      'BC 210 - Banca Colombia Cartagena',
                      'BC 311 - Bello',                   
                      'BC 88 - Cúcuta',                   
                      'BC 306 - Barrancabermeja',         
                      'BC 185 - Llano Grande Palmira',    
                      'BC 66 - Palmira',                  
                      'BC 205 - Villa Colombia',          
                      'BC 863 - Los Patios',              
                      'BC 764 - Jamundí',                 
                      'BC 424 - Honda',                   
                      'BC 613 - La America',              
                     #'BC 197 - Guatapuri',               
                      'BC 776 - Lebrija',                 
                      'BC 792 - Paseo del comercio',      
                      'BC 061 - Carrera Primera',         
                      'BC 496 - Iwanna',                  
                      'BC 453 - Pitalito',                
                      'BC 796 - Girón',                   
                      'BC 044 - Piedecuesta',             
                      'BC 799 - Floridablanca',           
                      'BC 834 - San Mateo',               
                      'BC 459 - Campo Alegre',            
                      'BC 678 - Paseo de la Castellana',  
                      'BC 741 - Calima',                  
                      'BC 495 - El Bosque',               
                      'BC 749 - Santa Monica',            
                      'BC 291 - Las Palmas',              
                      'BC 90 - Megamall',
                      'AA',
                      'ILU']},

      link = {
            'source':  [0,0,0,0,0,0,0,0,0,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0,
                        1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31], 


            'target':  [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
                        32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33],

            'value':   [El_Cacique,
                        Girardot,
                        Ventura_plaza,
                        Banca_Colombia_Cartagena,
                        Bello,
                        Cúcuta,
                        Barrancabermeja,
                        LLano_Grande_Palmira,
                        Palmira,
                        Villa_Colombia,
                        Los_Patios,
                        Jamundi,
                        Honda,
                        La_America,
                        Guatapuri,
                        Lebrija,
                        Paseo_del_comercio,
                        Carrera_primera,
                        Iwanna,
                        Pitalito,
                        Giron,
                        Piedecuesta,
                        Floridablanca,
                        San_Mateo,
                        Campo_Alegre,
                        Paseo_de_la_castellana,
                        Calima,
                        El_Bosque,
                        Santa_Monica,
                        Las_Palmas,
                        Megamall,

                        El_Cacique_aa,
                        El_Cacique_ilu,
                        Girardot_aa,
                        Girardot_ilu,
                        Ventura_plaza_aa,
                        Ventura_plaza_ilu,
                        Banca_Colombia_Cartagena_aa,
                        Banca_Colombia_Cartagena_ilu,
                        Bello_ilu,
                        Bello_aa,
                        Cúcuta_aa,
                        Cúcuta_ilu,
                        Barrancabermeja_aa,
                        Barrancabermeja_ilu,
                        LLano_Grande_Palmira_aa,
                        LLano_Grande_Palmira_ilu,
                        Palmira_aa,
                        Palmira_ilu,
                        Villa_Colombia_aa,
                        Villa_Colombia_ilu,
                        Los_Patios_aa,
                        Los_Patios_ilu,
                        Jamundi_aa,
                        Jamundi_ilu,
                        Honda_aa,
                        Honda_ilu,
                        La_America_aa,
                        La_America_ilu,
                        Guatapuri_aa,
                        Guatapuri_ilu,
                        Lebrija_aa,
                        Lebrija_ilu,
                        Paseo_del_comercio_aa,
                        Paseo_del_comercio_ilu,
                        Carrera_primera_aa,
                        Carrera_primera_ilu,
                        Iwanna_aa,
                        Iwanna_ilu,
                        Pitalito_aa,
                        Pitalito_ilu,
                        Giron_aa,
                        Giron_ilu,
                        Piedecuesta_aa,
                        Piedecuesta_ilu,
                        Floridablanca_aa,
                        Floridablanca_ilu,
                        San_Mateo_aa,
                        San_Mateo_ilu,
                        Campo_Alegre_aa,
                        Campo_Alegre_ilu,
                        Paseo_de_la_castellana_aa,
                        Paseo_de_la_castellana_ilu,
                        Calima_aa,
                        Calima_ilu,
                        El_Bosque_aa,
                        El_Bosque_ilu,
                        Santa_Monica_aa,
                        Santa_Monica_ilu,
                        Las_Palmas_aa,
                        Las_Palmas_ilu,
                        Megamall_aa,
                        Megamall_ilu
                       ]},
)])

fig.update_layout(title_text="Diagrama Sankey consumo de energía kWh", font_size=12,width=750,height=500)


# Te invitamos a validar el comportamiento de cada sede a detalle en las siguientes páginas.
