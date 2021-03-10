


"""
passar os dados do risco_unif_ssv para uma tabela no sql
"""


import configparser
config = configparser.ConfigParser()
config.read('access.ini')


import psycopg2

conn = psycopg2.connect(
    host=config["postgres"]["host"],
    database=config["postgres"]["database"],
    user=config["postgres"]["user"],
    password=config["postgres"]["password"])

cursor = conn.cursor()


import os
import pandas as pd
pd.options.display.max_columns = None

import numpy as np
import json

DATA_PATH = "./data"


from modulo_projeto_dados import redshift2pandas







caminho_csv = os.path.join(DATA_PATH, "risco3_unif_ssv_202012.sas7bdat.csv")
df_names = pd.read_csv(caminho_csv, nrows=0)
nome_variaveis = df_names.columns


with open("variaveis_interesse.json") as json_file:
    variaveis_interesse = json.load(json_file)


# variaveis que quero manter
lista_vars_keep = [x for x in variaveis_interesse.keys() if variaveis_interesse[x]]


pos = 10
df = pd.read_csv(caminho_csv, nrows=10,skiprows=pos, names=nome_variaveis, usecols=lista_vars_keep)


def traduzir_postgres_datatype(nome_var, df):
    """
    nome_var: nome da variável
    """
    dtype = df[nome_var].dtype
    
    if dtype == float:
        return "REAL"
    elif dtype == int:
        return "INTEGER"
    elif dtype == object:
        return "VARCHAR"




data_types = [traduzir_postgres_datatype(nome_var, df) for nome_var in df.columns]

lista_query = [x+" "+y for x,y in zip(df.columns, data_types)]


query = f"""
CREATE TABLE tabela_emissao (
    {", ".join(lista_query)}
);
"""
cursor.execute(query)
conn.commit()
print("Tabela criada com sucesso no PostgreSQL ")








































