from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob

URL = 'http://200.152.38.155/CNPJ/'

ZIP_FOLDER = r"dados-publicos-zip"

if len(glob.glob(os.path.join(vars,'*.zip'))):
  print(f'Há arquivos zip na pasta {ZIP_FOLDER}. Apague ou mova esses arquivos zip e tente novamente')
  sys.exit()
       
page = requests.get(URL)    
data = page.text
soup = BeautifulSoup(data)
lista = []

for link in soup.find_all('a'):
  if str(link.get('href')).endswith('.zip'): 
    cam = link.get('href')
    if not cam.startswith('http'):
      print(URL+cam)
      lista.append(URL+cam)
    else:
      print(cam)
      lista.append(cam)
  
def bar_progress(current, total):
  if total>=2**20:
    tbytes='Megabytes'
    unit = 2**20
  else:
    tbytes='kbytes'
    unit = 2**10
  progress_message = f"Downloading...: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unit, total//unit)
  sys.stdout.write("\r" + progress_message)
  sys.stdout.flush()
  
for k, url in enumerate(lista):
  wget.download(url, out=os.path.join(ZIP_FOLDER, os.path.split(url)[1]), bar=bar_progress)
    
print('\n\n'+ time.asctime() + f' Finalizado: baixados {len(lista)} arquivos.')