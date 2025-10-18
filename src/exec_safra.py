import sqlite3
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", '-d', help='Data referência de safra. Formato YYYY-MM-DD', default='2017-04-01')

args = parser.parse_args()
date = args.date

# DATA_DIR aponta para a pasta src
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
# BASE_DIR sobe um nível (para a pasta raiz do projeto)
BASE_DIR = os.path.dirname(DATA_DIR)
# DB_PATH aponta para data/db_olist.sqlite a partir da pasta raiz
DB_PATH = os.path.join(BASE_DIR, 'data', 'db_olist.sqlite')

def import_query(path, **kwargs):
    with open(path, 'r', **kwargs) as file_open:
        result = file_open.read()
    return result

def connect_db():
    return sqlite3.connect(DB_PATH)

# Caminho para o arquivo query.sql (assumindo que está na mesma pasta src)
query_path = os.path.join(DATA_DIR, 'query.sql')
query = import_query(query_path)
query = query.format(date=date)  # use the date variable from args

con = connect_db()
cursor = con.cursor()

try:
    print("Tentando deletar a tabela...")
    base_query = 'CREATE TABLE tb_book_sellers as\n {query}'
    cursor.execute(base_query.format(query=query))

except Exception as e:
    print(f"Erro ao criar tabela: {e}\n")
    print("Tentando inserir dados na tabela...\n")
    base_query = 'INSERT INTO tb_book_sellers \n {query}'
    cursor.execute(base_query.format(query=query))
    print("Dados inseridos.")

# Commit the transaction
con.commit()

# Close the connection
con.close()