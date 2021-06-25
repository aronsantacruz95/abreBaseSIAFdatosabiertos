# fuente
# https://youtu.be/xKMyk4wDHnQ

import numpy as np
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# seteamos la ruta
DIR='C:/Users/ARON SANTA CRUZ/Documents/Bases/Originales/MEF-SIAF/2019'
# definimos el nombre del csv que deseamos filtrar
FILE='/2019-Gasto.csv'
# definimos el nombre del archivo producto del filtro
FILEOUTT='/2019_SECTOR_36.csv'

file='{}{}'.format(DIR,FILE)
fileoutt='{}{}'.format(DIR,FILEOUTT)
print('Directorio: {}'.format(file))
print('Directorio: {}'.format(fileoutt))

# variables y primeras dos filas
pd.set_option('display.max_columns', None)
print(pd.read_csv(file,nrows=1))

# La solución para cargar archivos BIEN pesados en python es:
#   1. Crear un "Connector" para la BD
#   2. Construir (cargar) la BD mediante "chunking"
#   3. Construir el Pandas DataFrame para propósitos de análisis de la BD
#   usando los query de SQL

# 1. Crear un "Connector" para la BD
# ==================================

csv_database=create_engine('sqlite:///2019-Gasto.db')

# 2. Construir (cargar) la BD mediante "chunking"
# ===============================================

chunksize=10000
i=0
j=0
for df in pd.read_csv(file,chunksize=chunksize,iterator=True):
    # removemos los espacios de los nombres de las variables
    # para que SQL corra bien
    df=df.rename(columns={c: c.replace(' ','') for c in df.columns})
    df.index += j
    
    df.to_sql('intento01',csv_database,if_exists='append')
    j=df.index[-1]+1
    
    print('| index: {}'.format(j))

# 3. Construir el Pandas DataFrame
# ================================

# introduce la instrucción en lenguaje SQL
df=pd.read_sql_query('SELECT * FROM intento01 WHERE SECTOR=36',csv_database)

df.columns

df.head(6)

df=df[['ANO_EJE','MES_EJE','TIPO_GOBIERNO','TIPO_GOBIERNO_NOMBRE','SECTOR','SECTOR_NOMBRE','PLIEGO','PLIEGO_NOMBRE','SEC_EJEC','EJECUTORA','EJECUTORA_NOMBRE','DEPARTAMENTO_EJECUTORA','DEPARTAMENTO_EJECUTORA_NOMBRE','PROVINCIA_EJECUTORA','PROVINCIA_EJECUTORA_NOMBRE','DISTRITO_EJECUTORA','DISTRITO_EJECUTORA_NOMBRE','SEC_FUNC','PROGRAMA_PPTO','PROGRAMA_PPTO_NOMBRE','TIPO_ACT_PROY','PRODUCTO_PROYECTO','PRODUCTO_PROYECTO_NOMBRE','ACTIVIDAD_ACCION_OBRA','ACTIVIDAD_ACCION_OBRA_NOMBRE','FUNCION','FUNCION_NOMBRE','DIVISION_FUNCIONAL','DIVISION_FUNCIONAL_NOMBRE','GRUPO_FUNCIONAL','GRUPO_FUNCIONAL_NOMBRE','META','FINALIDAD','META_NOMBRE','DEPARTAMENTO_META','DEPARTAMENTO_META_NOMBRE','FUENTE_FINANC','FUENTE_FINANC_NOMBRE','RUBRO','RUBRO_NOMBRE','TIPO_RECURSO','TIPO_RECURSO_NOMBRE','CATEG_GASTO','CATEG_GASTO_NOMBRE','TIPO_TRANSACCION','GENERICA','GENERICA_NOMBRE','SUBGENERICA','SUBGENERICA_NOMBRE','SUBGENERICA_DET','SUBGENERICA_DET_NOMBRE','ESPECIFICA','ESPECIFICA_NOMBRE','ESPECIFICA_DET','ESPECIFICA_DET_NOMBRE','MONTO_PIA','MONTO_PIM','MONTO_CERTIFICADO','MONTO_COMPROMETIDO_ANUAL','MONTO_COMPROMETIDO','MONTO_DEVENGADO','MONTO_GIRADO']]

df.to_csv(fileoutt,sep='|')