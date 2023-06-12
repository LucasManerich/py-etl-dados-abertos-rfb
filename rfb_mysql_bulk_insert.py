import pandas as pd, sqlalchemy, glob, time, dask.dataframe as dd
import os, sys

#%% DEFINA os par창metros do servidor.
dbname = 'trebiico_w7cnpj_producao'
username = 'trebiico_w7cnpj'
password = 'xxx'
host = 'localhost'

pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos"

engine = sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')


#arquivos_a_zipar = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))
#import zipfile

#for arq in arquivos_a_zipar:
#    print('descompactando ' + arq)
#    with zipfile.ZipFile(arq, 'r') as zip_ref:
#        zip_ref.extractall(pasta_saida)
        

sqlTabelas = '''
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
        cnpj_basico VARCHAR(8)
        identificador_de_socio VARCHAR(1)
        nome_socio VARCHAR(200)
        cnpj_cpf_socio VARCHAR(14)
        qualificacao_socio VARCHAR(2)
        data_entrada_sociedade VARCHAR(8)
        pais VARCHAR(3)
        representante_legal VARCHAR(11)
        nome_representante VARCHAR(200)
        qualificacao_representante_legal VARCHAR(2)
        faixa_etaria VARCHAR(1)
    );
    '''

print('Inicio sqlTabelas:', time.asctime())
for k, sql in enumerate(sqlTabelas.split(';')):
    if not sql.strip():
        continue
    print('-'*20 + f'\nexecutando parte {k}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqlTabelas...', time.asctime())

#%%

def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivo = list(glob.glob(os.path.join(pasta_saida, '*' + extensaoArquivo)))[0]
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
    dtab.to_sql(nomeTabela, engine, if_exists='append', index=None)

carregaTabelaCodigo('.CNAECSV','cnae_raw')
carregaTabelaCodigo('.MOTICSV', 'motivo_raw')
carregaTabelaCodigo('.MUNICCSV', 'municipio_raw')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica_raw')
carregaTabelaCodigo('.PAISCSV', 'paisv')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio_raw')

#%%

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


def carregaTipo(nome_tabela, tipo, colunas):
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas, #nrows=1000,
                         encoding='latin1', dtype=str,
                         na_filter=None)
        print('to_sql...', time.asctime())
        ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', dtype=sqlalchemy.sql.sqltypes.String)
        print('fim parcial...', time.asctime())


carregaTipo('estabelecimento_raw', '.ESTABELE', colunas_estabelecimento)
carregaTipo('socio_raw', '.SOCIOCSV', colunas_socios)
carregaTipo('empresa_raw', '.EMPRECSV', colunas_empresas)
carregaTipo('simples_raw', '.SIMPLES.CSV.*', colunas_simples)


print('FIM!!!', time.asctime())
