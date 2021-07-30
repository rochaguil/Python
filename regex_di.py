import pandas as pd
import json

texto = ''
texto += 'a'
with open('texto.txt') as texto:
    valor = texto.read()
    texto = (valor)
    
import re
re.findall(r'COD.: ([\d]+[-]?[\d]+)', texto)
re.findall(r'UNIDADE ([\d+],[\d]+)' , texto)
re.findall(r'UNIDADE [\d+][,.][\d]+ [\d]+,[\d]+ .*' , texto)
x = re.findall(r'UNIDADE [0-9].* .*' , texto)
prod = re.findall(r'C[OÓ]D[\.\:][\.\:]?[ ]?([\d]+[-]?[\d]+)', texto)
prod = len(re.findall(r'C[OÓ].*GAME READY', texto))
len(prod)
x
import numpy as np
table = []
for i in np.array(x):
    table.append((i).split(' '))
    
x = pd.DataFrame(table, columns = [0,'quantidade','peso','valor','valor total'])
x = x.apply(lambda x: x.str.replace('.','').str.replace(',','.'),axis=1)
x = x.iloc[:,1:].apply(lambda x: x.astype(float),axis=1)
x.to_clipboard(decimal=',')
pd.Series(prod).to_clipboard()




import pandas as pd
texto = list(set((re.findall(r'UNIDADE[\s]{2}.*[\s]{2}.*[\s]{2}.*[\s]{2}.*C[OÓ]D.*', parsed))))
set((re.findall(r'UNIDADE[\s]{2}.*[\s]{2}.*[\s]{2}.*[\s]{2}.*C[OÓ]D.*', parsed)))
table = []
for i in (texto):
    table.append([i])

for i in table:
    i[0].replace('\n','')

with open('upload.txt','w') as f:
    for i in np.array(table):
        f.write(i[0].replace('\n','') + '\n')
        
