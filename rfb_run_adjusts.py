import time
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

dbname = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')

FULL_SQL = '''
DROP TABLE IF EXISTS qtd_estabelecimento_uf;
CREATE TABLE qtd_estabelecimento_uf (
  quantidade INTEGER,
  uf VARCHAR(2)
);

DROP TABLE IF EXISTS qtd_estabelecimento_uf_municipio;
CREATE TABLE qtd_estabelecimento_uf_municipio (
  quantidade INTEGER,
  uf VARCHAR(2),
  codigo_municipio VARCHAR(4),
  nome_municipio VARCHAR(200),
);


INSERT INTO qtd_estabelecimento_uf_municipio (quantidade, uf, codigo_municipio, nome_municipio) 
WITH origem_municipios as (
  SELECT count(1) as quantidade, uf as uf, municipio as codigo_municipio 
  FROM estabelecimento_raw 
  GROUP BY uf, municipio 
)
SELECT 
  origem_municipios.quantidade, 
  origem_municipios.uf, 
  municipio_raw.codigo, 
  municipio_raw.descricao 
FROM origem_municipios 
JOIN municipio_raw ON municipio_raw.codigo = origem_municipios.codigo_municipio;


INSERT INTO qtd_estabelecimento_uf (quantidade, uf) 
SELECT count(1) as quantidade, uf FROM estabelecimento_raw GROUP BY uf;

CREATE TABLE referencia_ultima_carga (data_hora TIMESTAMP);
INSERT INTO referencia_ultima_carga VALUES (CURRENT_TIMESTAMP);
'''

engine = sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')

for k, sql in enumerate(FULL_SQL.split(';')):
  if not sql.strip():
    continue
  engine.execute(sql)

print('FIM!!!', time.asctime())
