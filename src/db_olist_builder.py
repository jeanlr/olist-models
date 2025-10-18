import pandas as pd
import os
from sqlalchemy import create_engine, text, inspect

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# ======================================================
# 1. Conexão com o banco de dados SQLite
# ======================================================
print("🔹 [1/7] Iniciando conexão com o banco de dados...")
db_path = os.path.join(DATA_DIR, 'db_olist.sqlite')

# Remove o banco existente para recriação completa
if os.path.exists(db_path):
    print("🗑️  Removendo banco de dados existente...")
    os.remove(db_path)

db = create_engine(f'sqlite:///{db_path}', echo=False)
conn = db.connect()
print("✅ Conexão estabelecida com sucesso!")

# ======================================================
# 2. Leitura dos arquivos CSV
# ======================================================
print("🔹 [2/7] Lendo arquivos CSV...")
df_orders = pd.read_csv(os.path.join(DATA_DIR, 'olist_orders_dataset.csv'), encoding='utf-8')
df_items = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'), encoding='utf-8')
df_customers = pd.read_csv(os.path.join(DATA_DIR, 'olist_customers_dataset.csv'), encoding='utf-8')
df_sellers = pd.read_csv(os.path.join(DATA_DIR, 'olist_sellers_dataset.csv'), encoding='utf-8')
df_products = pd.read_csv(os.path.join(DATA_DIR, 'olist_products_dataset.csv'), encoding='utf-8')
df_reviews = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_reviews_dataset.csv'), encoding='utf-8')
df_payments = pd.read_csv(os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'), encoding='utf-8')
df_geolocation = pd.read_csv(os.path.join(DATA_DIR, 'olist_geolocation_dataset.csv'), encoding='utf-8')
df_category = pd.read_csv(os.path.join(DATA_DIR, 'product_category_name_translation.csv'), encoding='utf-8')
print("✅ Leitura dos datasets concluída com sucesso!")

# ======================================================
# 3. Funções auxiliares CORRIGIDAS
# ======================================================
print("🔹 [3/7] Definindo funções auxiliares para criação e inserção...")

def create_table(connection, table_name, schema):
    """Cria tabela com commit explícito"""
    connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
    connection.execute(text(schema))
    connection.commit()
    return True

def insert_data(connection, table_name, dataframe, chunksize=1000):
    """
    Insere dados em chunks para evitar limite de parâmetros do SQLite
    """
    # Remove duplicatas baseadas na primary key antes de inserir
    original_size = len(dataframe)
    if table_name == 'tb_reviews':
        dataframe = dataframe.drop_duplicates(subset=['review_id'], keep='first')
        print(f"📊 Reviews após remoção de duplicatas: {len(dataframe)} registros (removidos {original_size - len(dataframe)})")
    elif table_name == 'tb_orders':
        dataframe = dataframe.drop_duplicates(subset=['order_id'], keep='first')
    elif table_name == 'tb_products':
        dataframe = dataframe.drop_duplicates(subset=['product_id'], keep='first')
    elif table_name == 'tb_sellers':
        dataframe = dataframe.drop_duplicates(subset=['seller_id'], keep='first')
    
    # Insere os dados em chunks
    print(f"📥 Inserindo {len(dataframe)} registros em {table_name} (chunksize={chunksize})...")
    
    total_inserted = 0
    for i in range(0, len(dataframe), chunksize):
        chunk = dataframe.iloc[i:i + chunksize]
        
        # SEMPRE usar 'append' pois a tabela já foi criada
        chunk.to_sql(
            table_name, 
            con=connection, 
            if_exists='append',  # SEMPRE append
            index=False
        )
        connection.commit()  # Commit após cada chunk
        
        total_inserted += len(chunk)
        if (i + chunksize) < len(dataframe):
            print(f"   ✅ {total_inserted}/{len(dataframe)} registros inseridos...")
    
    print(f"✅ {table_name} populada com sucesso! Total: {total_inserted} registros")
    return total_inserted

print("✅ Funções auxiliares carregadas!")

# ======================================================
# 4. Schemas corretos (mantenha igual)
# ======================================================
print("🔹 [4/7] Criando schemas das tabelas...")
schema_orders = '''
CREATE TABLE tb_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
)
'''

schema_items = '''
CREATE TABLE tb_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price REAL,
    freight_value REAL
)
'''

schema_customers = '''
CREATE TABLE tb_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
)
'''

schema_sellers = '''
CREATE TABLE tb_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
)
'''

schema_products = '''
CREATE TABLE tb_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght REAL,
    product_description_lenght REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL
)
'''

schema_reviews = '''
CREATE TABLE tb_reviews (
    review_id TEXT PRIMARY KEY,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
)
'''

schema_payments = '''
CREATE TABLE tb_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL
)
'''

schema_geolocation = '''
CREATE TABLE tb_geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
)
'''

schema_category = '''
CREATE TABLE tb_category (
    product_category_name TEXT,
    product_category_name_english TEXT
)
'''
print("✅ Schemas criados!")

# ======================================================
# 5. Criação das tabelas
# ======================================================
print("🔹 [5/7] Criando tabelas no banco de dados SQLite...")
create_table(conn, 'tb_orders', schema_orders)
create_table(conn, 'tb_items', schema_items)
create_table(conn, 'tb_customers', schema_customers)
create_table(conn, 'tb_sellers', schema_sellers)
create_table(conn, 'tb_products', schema_products)
create_table(conn, 'tb_reviews', schema_reviews)
create_table(conn, 'tb_payments', schema_payments)
create_table(conn, 'tb_geolocation', schema_geolocation)
create_table(conn, 'tb_category', schema_category)
print("✅ Tabelas criadas com sucesso!")

# ======================================================
# 6. Inserção dos dados COM CHUNKS
# ======================================================
print("🔹 [6/7] Inserindo dados nas tabelas...")

# Para tabelas grandes, usar chunks menores
insert_data(conn, 'tb_orders', df_orders, chunksize=1000)
insert_data(conn, 'tb_items', df_items, chunksize=1000)
insert_data(conn, 'tb_customers', df_customers, chunksize=1000)
insert_data(conn, 'tb_sellers', df_sellers, chunksize=1000)
insert_data(conn, 'tb_products', df_products, chunksize=1000)
insert_data(conn, 'tb_reviews', df_reviews, chunksize=1000)
insert_data(conn, 'tb_payments', df_payments, chunksize=1000)

# Para a tabela geolocation que é muito grande, usar chunks ainda menores
insert_data(conn, 'tb_geolocation', df_geolocation, chunksize=500)

insert_data(conn, 'tb_category', df_category, chunksize=100)

print("✅ Dados inseridos com sucesso!")

# ======================================================
# 7. Verificação final
# ======================================================
print("🔹 [7/7] Verificando dados inseridos...")
tables = ['tb_orders', 'tb_items', 'tb_customers', 'tb_sellers', 'tb_products', 'tb_reviews', 'tb_payments', 'tb_geolocation', 'tb_category']
for table in tables:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
    count = result.scalar()
    print(f"📊 {table}: {count} registros")

# COMMIT FINAL para garantir
conn.commit()

print("🔹 Fechando conexão com o banco de dados...")
conn.close()
print("🎉 Banco de dados 'data/db_olist.sqlite' criado e populado com sucesso!")