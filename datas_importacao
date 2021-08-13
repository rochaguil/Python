# 'dataset' tem os dados de entrada para este script

dataset  = pd.read_clipboard()
colunas = ['N° PO', 'N° PROCESSO', 'AGENTE', 'DATA EMBARQUE', 'DATA CHEGADA',
       'DATA PRESENÇA CARGA', 'DATA REGISTRO DI', 'DATA DESEMBARAÇO', 'DATA LIBERAÇÃO', 'DATA COLETA', 'DATA ENTREGA'
       ]
dataset = dataset[colunas].drop_duplicates()

import pandas as pd
import re

tab = pd.DataFrame(columns=colunas)

for PO in dataset['N° PO'][:2]:
    try:
        if ',' in PO:
            sub_PO = PO.split(',')
        elif '/' in PO:
            sub_PO = PO.split('/')
        else:
            sub_PO = [PO.strip()]
    except:
        pass
    for sub in sub_PO:
        try:
            tab.loc[sub.strip(),'N° PO'] = sub.strip()
            for coluna in colunas[1:]:
                valor = dataset[dataset['N° PO'] == PO][coluna].values[0]
                tab.loc[sub.strip(),coluna] = valor
                #tab['N° PO'] == sub = valor
        except:
            pass
