# %%
# Importação de bibliotecas e configurações iniciais
from datetime import datetime
import sqlite3
from warnings import filterwarnings
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import TunedThresholdClassifierCV
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import itertools
from sklearn.metrics import confusion_matrix, classification_report, roc_curve, roc_auc_score, f1_score, make_scorer, accuracy_score, precision_score, recall_score, auc
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, RobustScaler, LabelEncoder
from category_encoders import TargetEncoder
from category_encoders.woe import WOEEncoder
from sklearn.feature_selection import SelectFromModel, SelectKBest, f_classif
import sys
sys.path.append(r'/home/jean/projetos/olist/global/')
from util import *
from lightgbm import LGBMClassifier
import pickle
import mlflow

# Configurações iniciais para ignorar warnings e exibir todas as colunas do pandas
filterwarnings('ignore')
pd.set_option('display.max_columns', None)

# %%
# Configuração do MLflow para rastreamento de experimentos
mlflow.set_tracking_uri('http://localhost:5000')

# Obtendo a versão mais recente do modelo registrado
client = mlflow.client.MlflowClient()
version = max([int(i.version) for i in client.get_latest_versions("olist_churn_sellers")])
version

# %%
# Carregando o modelo treinado mais recente do MLflow
loaded_model = mlflow.lightgbm.load_model(f"models:/olist_churn_sellers/{version}")

# %%
# Carregamento dos dados de treino e seleção das colunas finais
# Conectar ao banco SQLite
conn = sqlite3.connect('../../data/db_olist.sqlite')

# Consulta para pegar apenas os registros com a máxima data dt_ref
max_date_data_query = '''
SELECT * 
FROM tb_abt 
WHERE dt_ref = (SELECT MAX(dt_ref) FROM tb_abt)
'''

# Carregar apenas os dados da máxima data
abt_00 = pd.read_sql(max_date_data_query, conn)

# Fechar a conexão
conn.close()

# %%
# Criar uma cópia para teste, mas manter as colunas seller_id e dt_ref separadamente
df_test = abt_00.drop(columns=['seller_id', 'dt_ref', 'flag_churn_3m']).copy()

# Salvar os IDs e data de referência separadamente
seller_ids = abt_00['seller_id'].copy()
dt_refs = abt_00['dt_ref'].copy()

# %%
# Gerando metadados das features para identificar tipos e cardinalidades
metadados = generate_metadata(df=df_test, targets=[''], orderby='PC_NULOS')
metadados

# %%
# Separando features em categorias baseado nos metadados:
# - Categóricas com baixa cardinalidade (<20 categorias)
# - Categóricas com alta cardinalidade (>=20 categorias)
# - Numéricas
cat_features_low_card = metadados[(metadados['TIPO_FEATURE'] == 'object') & 
                                (metadados['CARDINALIDADE'] < 20)]['FEATURE'].tolist()

cat_features_high_card = metadados[(metadados['TIPO_FEATURE'] == 'object') & 
                                 (metadados['CARDINALIDADE'] >= 20)]['FEATURE'].tolist()

num_features = metadados[(metadados['TIPO_FEATURE'] != 'object')]['FEATURE'].tolist()

# %%
# Carregando e aplicando o pré-processador usado no treino
with open('../../artifacts/prd_preprocesssor_skl.pkl', 'rb') as f:
    preprocesssor = pickle.load(f)

# Transformando os dados de teste
X_test_processed = preprocesssor.transform(df_test)

# Recuperando os nomes das colunas após one-hot encoding
onehot_columns = []
if len(cat_features_low_card) > 0:
    onehot = preprocesssor.named_steps['preprocessor'].named_transformers_['cat_low'].named_steps['onehot']
    for i, col in enumerate(cat_features_low_card):
        cats = onehot.categories_[i]
        onehot_columns.extend([f"{col}_{cat}" for cat in cats])
        
# Juntando todas as colunas processadas
processed_columns = onehot_columns + cat_features_high_card + num_features
X_test_processed = pd.DataFrame(X_test_processed, columns=processed_columns)

# %%
# Aplicando a seleção final de features usadas no modelo
with open('../../artifacts/prd_selected_features_skl.pkl', 'rb') as f:
    selected_features = pickle.load(f)
X_test_processed = X_test_processed[selected_features]
X_test_processed.shape

# %%
# Obtendo as probabilidades de predição do modelo
proba = loaded_model.predict_proba(X_test_processed)[:, 1]
proba

# %%
# Criar o dataframe final com os resultados
df_final = pd.DataFrame({
    'seller_id': seller_ids,
    'dt_ref': dt_refs,
    'SCORE': proba,
    'DATA_SCORE': datetime.today().date()
})

# Ordenar por score decrescente
df_final = df_final.sort_values(by='SCORE', ascending=False)
df_final.head()

# %%
# Conectar ao banco SQLite
conn = sqlite3.connect('../../data/db_olist.sqlite')

# Criar a tabela se não existir e fazer o append dos dados
try:
    # Salvar o DataFrame na tabela (cria a tabela se não existir)
    df_final.to_sql(
        name='tb_scoring', 
        con=conn, 
        if_exists='append',  # Faz append dos novos dados
        index=False,         # Não incluir o índice do DataFrame
        method=None          # Usar método padrão de inserção
    )
    print("Dados inseridos com sucesso na tabela tb_scoring!")
    
except Exception as e:
    print(f"Erro ao inserir dados: {e}")

finally:
    # Fechar a conexão
    conn.close()
# %%
