from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob

url = 'http://200.152.38.155/CNPJ/'

pasta_compactados = r"dados-publicos-zip"

if len(glob.glob(os.path.join(pasta_compactados,'*.zip'))):
    print(f'Há arquivos zip na pasta {pasta_compactados}. Apague ou mova esses arquivos zip e tente novamente')
    sys.exit()
       
page = requests.get(url)    
data = page.text
soup = BeautifulSoup(data)
lista = []

print('Relação de Arquivos em ' + url)
for link in soup.find_all('a'):
    if str(link.get('href')).endswith('.zip'): 
        cam = link.get('href')
        if not cam.startswith('http'):
            print(url+cam)
            lista.append(url+cam)
        else:
            print(cam)
            lista.append(cam)
    
def bar_progress(current, total, width=80):
    if total>=2**20:
        tbytes='Megabytes'
        unidade = 2**20
    else:
        tbytes='kbytes'
        unidade = 2**10
    progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()
  
for k, url in enumerate(lista):
    print('\n' + time.asctime() + f' - item {k}: ' + url)
    wget.download(url, out=os.path.join(pasta_compactados, os.path.split(url)[1]), bar=bar_progress)
    
print('\n\n'+ time.asctime() + f' Finalizou!!! Baixou {len(lista)} arquivos.')