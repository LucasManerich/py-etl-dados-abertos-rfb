import glob
import time
import os
import zipfile
import pandas as pd
import sqlalchemy

# %% DEFINA os parâmetros do servidor.
dbname = 'trebiico_w7cnpj_producao'
username = 'trebiico_w7cnpj'
password = 'xxxxx'
host = 'localhost'

zip_folder = r"dados-publicos-zip"
output_folder = r"dados-publicos"

engine = sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')
to_unzip = list(glob.glob(os.path.join(zip_folder,r'*.zip')))

for file in to_unzip:
  print('Descompactando o arquivo: ' + file)
  with zipfile.ZipFile(file, 'r') as zip_ref:
    zip_ref.extractall(output_folder)


tableSql = '''
DROP TABLE IF EXISTS cnae_raw;
CREATE TABLE cnae_raw (
  codigo VARCHAR(7),
  descricao VARCHAR(200)
);
DROP TABLE IF EXISTS empresa_raw;
CREATE TABLE empresa_raw (
  cnpj_basico VARCHAR(8),
  razao_social VARCHAR(200),
  natureza_juridica VARCHAR(4),
  qualificacao_responsavel VARCHAR(2),
  capital_social VARCHAR(20),
  porte_empresa VARCHAR(2),
  ente_federativo_responsavel VARCHAR(50)
);

DROP TABLE IF EXISTS estabelecimento_raw;
CREATE TABLE estabelecimento_raw (
  cnpj_basico VARCHAR(8),
  cnpj_ordem VARCHAR(4),
  cnpj_dv VARCHAR(2),
  matriz_filial VARCHAR(1),
  nome_fantasia VARCHAR(200),
  situacao_cadastral VARCHAR(2),
  data_situacao_cadastral VARCHAR(8),
  motivo_situacao_cadastral VARCHAR(2),
  nome_cidade_exterior VARCHAR(200),
  pais VARCHAR(3),
  data_inicio_atividades VARCHAR(8),
  cnae_fiscal VARCHAR(7),
  cnae_fiscal_secundaria VARCHAR(1000),
  tipo_logradouro VARCHAR(20),
  logradouro VARCHAR(200),
  numero VARCHAR(10),
  complemento VARCHAR(200),
  bairro VARCHAR(200),
  cep VARCHAR(8),
  uf VARCHAR(2),
  municipio VARCHAR(4),
  ddd1 VARCHAR(4),
  telefone1 VARCHAR(8),
  ddd2 VARCHAR(4),
  telefone2 VARCHAR(8),
  ddd_fax VARCHAR(4),
  fax VARCHAR(8),
  correio_eletronico VARCHAR(200),
  situacao_especial VARCHAR(200),
  data_situacao_especial VARCHAR(8)
);

DROP TABLE IF EXISTS motivo_raw;
CREATE TABLE motivo_raw (
  codigo VARCHAR(2),
  descricao VARCHAR(200)
);
DROP TABLE IF EXISTS municipio_raw;
CREATE TABLE municipio_raw (
  codigo VARCHAR(4),
  descricao VARCHAR(200)
);

DROP TABLE IF EXISTS natureza_juridica_raw;
CREATE TABLE natureza_juridica_raw (
  codigo VARCHAR(4),
  descricao VARCHAR(200)
);

DROP TABLE IF EXISTS pais_raw;
CREATE TABLE pais_raw (
  codigo VARCHAR(3),
  descricao VARCHAR(200)
);

DROP TABLE IF EXISTS qualificacao_socio_raw;
CREATE TABLE qualificacao_socio_raw (
  codigo VARCHAR(2),
  descricao VARCHAR(200)
);

DROP TABLE IF EXISTS simples_raw;
CREATE TABLE simples_raw (
  cnpj_basico VARCHAR(8),
  opcao_simples VARCHAR(1),
  data_opcao_simples VARCHAR(8),
  data_exclusao_simples VARCHAR(8),
  opcao_mei VARCHAR(1),
  data_opcao_mei VARCHAR(8),
  data_exclusao_mei VARCHAR(8)
);

DROP TABLE IF EXISTS socio_raw;
CREATE TABLE socio_raw (
  cnpj_basico VARCHAR(8),
  identificador_de_socio VARCHAR(1),
  nome_socio VARCHAR(200),
  cnpj_cpf_socio VARCHAR(14),
  qualificacao_socio VARCHAR(2),
  data_entrada_sociedade VARCHAR(8),
  pais VARCHAR(3),
  representante_legal VARCHAR(11),
  nome_representante VARCHAR(200),
  qualificacao_representante_legal VARCHAR(2),
  faixa_etaria VARCHAR(1)
);
'''

for k, sql in enumerate(tableSql.split(';')):
  if not sql.strip():
    continue
  engine.execute(sql)


def perform_small_table(file_pattern, table_name):
  file = list(glob.glob(os.path.join(output_folder, file_pattern)))[0]
  dtab = pd.read_csv(file, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
  dtab.to_sql(table_name, engine, if_exists='append', index=None)

perform_small_table('*.CNAECSV','cnae_raw')
perform_small_table('*.MOTICSV', 'motivo_raw')
perform_small_table('*.MUNICCSV', 'municipio_raw')
perform_small_table('*.NATJUCSV', 'natureza_juridica_raw')
perform_small_table('*.PAISCSV', 'pais_raw')
perform_small_table('*.QUALSCSV', 'qualificacao_socio_raw')

colunas_estabelecimento = [
  'cnpj_basico',
  'cnpj_ordem',
  'cnpj_dv',
  'matriz_filial',
  'nome_fantasia',
  'situacao_cadastral',
  'data_situacao_cadastral',
  'motivo_situacao_cadastral',
  'nome_cidade_exterior',
  'pais',
  'data_inicio_atividades',
  'cnae_fiscal',
  'cnae_fiscal_secundaria',
  'tipo_logradouro',
  'logradouro',
  'numero',
  'complemento',
  'bairro',
  'cep',
  'uf',
  'municipio',
  'ddd1',
  'telefone1',
  'ddd2',
  'telefone2',
  'ddd_fax',
  'fax',
  'correio_eletronico',
  'situacao_especial',
  'data_situacao_especial'
]

colunas_empresas = [
  'cnpj_basico',
  'razao_social',
  'natureza_juridica',
  'qualificacao_responsavel',
  'capital_social',
  'porte_empresa',
  'ente_federativo_responsavel'
]

colunas_socios = [
  'cnpj_basico',
  'identificador_de_socio',
  'nome_socio',
  'cnpj_cpf_socio',
  'qualificacao_socio',
  'data_entrada_sociedade',
  'pais',
  'representante_legal',
  'nome_representante',
  'qualificacao_representante_legal',
  'faixa_etaria'
]

colunas_simples = [
  'cnpj_basico',
  'opcao_simples',
  'data_opcao_simples',
  'data_exclusao_simples',
  'opcao_mei',
  'data_opcao_mei',
  'data_exclusao_mei'
]


def perform_big_table(table_name, file_pattern, columns):
  fileList = list(glob.glob(os.path.join(output_folder, file_pattern)))
  for file in fileList:
    with pd.read_csv(file, sep=';', header=None, names=columns, chunksize=150000, encoding='latin1', dtype=str, na_filter=None) as reader:
      for chunk in reader:
        chunk.to_sql(table_name, engine, index=None, if_exists='append', method='multi', chunksize=500, dtype=sqlalchemy.sql.sqltypes.TEXT)


perform_big_table('estabelecimento_raw', '*.ESTABELE', colunas_estabelecimento)
perform_big_table('empresa_raw', '*.EMPRECSV', colunas_empresas)
perform_big_table('socio_raw', '*.SOCIOCSV', colunas_socios)
perform_big_table('simples_raw', '*.SIMPLES.CSV.*', colunas_simples)

print('FIM!!!', time.asctime())
